// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package snaprecv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/time/rate"
)

// SSTSnapshotStorage provides an interface to create scratches and owns the
// directory of scratches created. A scratch manages the SSTs created during a
// specific snapshot.
type SSTSnapshotStorage struct {
	env     *fs.Env
	limiter *rate.Limiter
	dir     string
	mu      struct {
		syncutil.Mutex
		rangeRefCount map[roachpb.RangeID]int
	}
}

// NewSSTSnapshotStorage creates a new SST snapshot storage.
// **输入**：
// - `engine storage.Engine`：Pebble 存储引擎实例
// - `limiter *rate.Limiter`：全局 IO 速率限制器
//
// **输出**：
// - `SSTSnapshotStorage` 结构体（**注意不是指针**）
//
// **关键点 1：为什么返回值不是指针？**
//
// ```go
// // store.go 中的使用
// s.sstSnapshotStorage = snaprecv.NewSSTSnapshotStorage(...)
// ```
//
// 这是 Go 的一个常见模式：
// - 返回结构体值（而不是指针）可以避免堆分配
// - SSTSnapshotStorage 结构体本身很小（3 个字段 + 一个嵌套结构体）
// - 由于 rangeRefCount 是 map（引用类型），实际的 map 数据仍然在堆上
// - 这种设计在拷贝开销小且需要值语义时很常见
func NewSSTSnapshotStorage(engine storage.Engine, limiter *rate.Limiter) SSTSnapshotStorage {
	return SSTSnapshotStorage{
		env:     engine.Env(),
		limiter: limiter,
		dir:     filepath.Join(engine.GetAuxiliaryDir(), "sstsnapshot"),
		mu: struct {
			syncutil.Mutex
			rangeRefCount map[roachpb.RangeID]int
		}{rangeRefCount: make(map[roachpb.RangeID]int)},
	}
}

// NewScratchSpace creates a new storage scratch space for SSTs for a specific
// snapshot.
func (s *SSTSnapshotStorage) NewScratchSpace(
	rangeID roachpb.RangeID, snapUUID uuid.UUID, st *cluster.Settings,
) *SSTSnapshotStorageScratch {
	// Step 1: 增加引用计数（加锁保护）
	s.mu.Lock()
	//为什么不能使用 atomic.AddInt32？
	//// ❌ 错误示例
	//atomic.AddInt32(&s.mu.rangeRefCount[rangeID], 1)
	//​
	//问题：
	//- map[...]int 不是线程安全的
	//- 即使每个元素用原子操作，map 本身的增删也需要锁
	s.mu.rangeRefCount[rangeID]++
	s.mu.Unlock()
	// Step 2: 构造快照专属目录路径
	snapDir := filepath.Join(s.dir, strconv.Itoa(int(rangeID)), snapUUID.String())
	// 例如：/data/auxiliary/sstsnapshot/123/550e8400-e29b-41d4-a716-446655440000
	// Step 3: 返回 Scratch 对象（此时目录尚未创建）
	//返回的 scratch 对象的 dirCreated 为 false
	return &SSTSnapshotStorageScratch{
		storage: s,
		st:      st,
		rangeID: rangeID,
		snapDir: snapDir,
	}
}

// Clear removes all created directories and SSTs.
func (s *SSTSnapshotStorage) Clear() error {
	return s.env.RemoveAll(s.dir)
}

// scratchClosed is called when an SSTSnapshotStorageScratch created by this
// SSTSnapshotStorage is closed. This method handles any cleanup of range
// directories if all SSTSnapshotStorageScratches corresponding to a range
// have closed.
func (s *SSTSnapshotStorage) scratchClosed(rangeID roachpb.RangeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Step 1: 减少引用计数
	val := s.mu.rangeRefCount[rangeID]
	if val <= 0 {
		panic("inconsistent scratch ref count")
	}
	val--
	s.mu.rangeRefCount[rangeID] = val
	// Step 2: 如果没有更多活跃快照，清理 Range 目录
	if val == 0 {
		delete(s.mu.rangeRefCount, rangeID)
		// Suppressing an error here is okay, as orphaned directories are at worst
		// a performance issue when we later walk directories in pebble.Capacity()
		// but not a correctness issue.
		// 删除 Range 目录（抑制错误）
		// 注释：孤儿目录最多影响 pebble.Capacity() 性能，不影响正确性
		_ = s.env.RemoveAll(filepath.Join(s.dir, strconv.Itoa(int(rangeID))))
	}
}

// SSTSnapshotStorageScratch keeps track of the SST files incrementally created
// when receiving a snapshot. Each scratch is associated with a specific
// snapshot.
type SSTSnapshotStorageScratch struct {
	storage    *SSTSnapshotStorage
	st         *cluster.Settings
	rangeID    roachpb.RangeID
	ssts       []string
	snapDir    string
	dirCreated bool
	closed     bool
}

func (s *SSTSnapshotStorageScratch) filename(id int) string {
	return filepath.Join(s.snapDir, fmt.Sprintf("%d.sst", id))
}

func (s *SSTSnapshotStorageScratch) createDir() error {
	err := s.storage.env.MkdirAll(s.snapDir, os.ModePerm)
	s.dirCreated = s.dirCreated || err == nil
	return err
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time. A nonzero value for
// bytesPerSync will sync dirty data periodically as it is written. The syncing
// does not provide persistency guarantees, but is used to smooth out disk
// writes. Sync() must be called for data persistence.
func (s *SSTSnapshotStorageScratch) NewFile(
	ctx context.Context, bytesPerSync int64,
) (*SSTSnapshotStorageFile, error) {
	if s.closed {
		return nil, errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
	}
	// 分配文件 ID（从 0 开始递增）

	id := len(s.ssts)
	filename := s.filename(id) // {snapDir}/{id}.sst
	// 追加到文件列表（即使文件尚未创建）
	s.ssts = append(s.ssts, filename)
	// 返回 File 对象

	f := &SSTSnapshotStorageFile{
		scratch:      s,
		filename:     filename,
		ctx:          ctx,
		bytesPerSync: bytesPerSync,
	}
	return f, nil
}

// WriteSST creates an SST populated with the given write function, and writes
// it to a file. Does nothing if no data is written.
func (s *SSTSnapshotStorageScratch) WriteSST(
	ctx context.Context, write func(context.Context, storage.Writer) error,
) error {
	if s.closed {
		return errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
	}

	// TODO(itsbilal): Write to SST directly rather than buffer in a MemObject.
	sstFile := &storage.MemObject{}
	w := storage.MakeIngestionSSTWriter(ctx, s.st, sstFile)
	defer w.Close()
	if err := write(ctx, &w); err != nil {
		return err
	}
	if err := w.Finish(); err != nil {
		return err
	}
	if w.DataSize == 0 {
		return nil
	}

	f, err := s.NewFile(ctx, 512<<10 /* 512 KB */)
	if err != nil {
		return err
	}
	if err := f.Write(sstFile.Data()); err != nil {
		f.Abort()
		return err
	}
	return f.Finish()
}

// SSTs returns the names of the files created.
func (s *SSTSnapshotStorageScratch) SSTs() []string {
	return s.ssts
}

// Close removes the directory and SSTs created for a particular snapshot.
func (s *SSTSnapshotStorageScratch) Close() error {
	if s.closed {
		return nil
	} // 幂等性：重复关闭不报错
	s.closed = true
	// Step 1: 通知父对象（引用计数 -1）
	defer s.storage.scratchClosed(s.rangeID)
	// Step 2: 删除快照目录（包括所有 SST 文件）
	return s.storage.env.RemoveAll(s.snapDir)
}

// SSTSnapshotStorageFile is an SST file managed by a
// SSTSnapshotStorageScratch.
type SSTSnapshotStorageFile struct {
	scratch      *SSTSnapshotStorageScratch
	created      bool     // 延迟创建标记
	file         vfs.File // Pebble 的虚拟文件系统接口
	filename     string   // 文件路径：{snapDir}/{id}.sst
	ctx          context.Context
	bytesPerSync int64 // 每写入 N 字节后调用 Sync()（平滑 IO）
}

var _ objstorage.Writable = (*SSTSnapshotStorageFile)(nil)

// 首次写入时：
// - scratch.dirCreated 变为 true
// - 磁盘上创建目录：/data/auxiliary/sstsnapshot/123/snap-123/
// - 磁盘上创建文件：/data/auxiliary/sstsnapshot/123/snap-123/0.sst
// - file.created 变为 true
func (f *SSTSnapshotStorageFile) ensureFile() error {
	if f.created {
		if f.file == nil {
			return errors.Errorf("file has already been closed")
		}
		return nil // 已创建，直接返回
	}
	// Step 1: 如果目录未创建，先创建目录
	if !f.scratch.dirCreated {
		if err := f.scratch.createDir(); err != nil {
			return err
		}
	}
	if f.scratch.closed {
		return errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
	}
	var err error
	// Step 2: 创建文件
	if f.bytesPerSync > 0 {
		// 使用同步写入（定期调用 Sync）
		f.file, err = fs.CreateWithSync(f.scratch.storage.env, f.filename, int(f.bytesPerSync), fs.RaftSnapshotWriteCategory)
	} else {
		// 普通写入
		f.file, err = f.scratch.storage.env.Create(f.filename, fs.RaftSnapshotWriteCategory)
	}
	if err != nil {
		return err
	}
	f.created = true
	return nil
}

func (f *SSTSnapshotStorageFile) StartMetadataPortion() error { return nil }

// Write is part of objstorage.Writable; it writes contents to the file while
// respecting the limiter passed into SSTSnapshotStorageScratch. Writing empty
// contents is okay and is treated as a noop.
// Cannot be called after Finish or Abort.
func (f *SSTSnapshotStorageFile) Write(contents []byte) error {
	if len(contents) == 0 {
		return nil
	} // 空写入不触发创建
	// Step 1: 确保文件已创建
	if err := f.ensureFile(); err != nil {
		return err
	}
	// Step 2: 速率限制（关键！）
	if err := kvserverbase.LimitBulkIOWrite(f.ctx, f.scratch.storage.limiter, len(contents)); err != nil {
		return err // 返回 context.Canceled 或速率限制错误
	}
	// Write always returns an error if it can't write all the contents.
	// Step 3: 实际写入
	_, err := f.file.Write(contents)
	return err
}

// Finish is part of the objstorage.Writable interface.
// **状态变化**：
// - 文件数据落盘（持久化）
// - `f.file` 设置为 `nil`（防止重复关闭）
//
// **关键点**：
// - 如果 `Finish()` 返回错误，上层逻辑会调用 `scratch.Close()` 清理所有文件
// - 空文件检查确保不会生成无效的 SST（Pebble 无法 ingest 空 SST）
func (f *SSTSnapshotStorageFile) Finish() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	// 检查：空文件是错误的（SST 必须包含数据）
	if !f.created {
		return errors.New("file is empty")
	}
	// Step 1: 同步数据到磁盘
	errSync := f.file.Sync()
	// Step 2: 关闭文件句柄
	errClose := f.file.Close()
	f.file = nil
	// Step 3: 返回错误（优先返回 Sync 错误）
	if errSync != nil {
		return errSync
	}
	return errClose
}

// Abort is part of the objstorage.Writable interface.
func (f *SSTSnapshotStorageFile) Abort() {
	if f.file != nil {
		_ = f.file.Close()
		f.file = nil
	}
}
