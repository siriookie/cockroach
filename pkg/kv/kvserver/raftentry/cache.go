// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package raftentry provides a cache for entries to avoid extra
// deserializations.
package raftentry

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Cache is a specialized data structure for storing deserialized raftpb.Entry
// values tailored to the access patterns of the storage package.
// Cache is safe for concurrent access.
type Cache struct {
	// ======== 监控指标 ========
	// - Size: 当前entry数量
	// - Bytes: 当前字节数
	// - Accesses: 总访问次数
	// - Hits: 命中次数
	// - ReadBytes: 累计读取字节数
	metrics Metrics
	// ======== 容量配置 ========
	// 例如：128MB = 134,217,728 bytes
	maxBytes int32 // 最大字节数（不可变）

	// accessed with atomics
	// ======== 原子统计 ========
	// 为什么用atomic？
	// - 高频更新（每次Add/Clear）
	// - 读取无锁（metrics查询）
	// - 与partition.size配合实现无锁统计
	bytes   int32 // 当前使用字节数（atomic）
	entries int32 // 当前entry数量（atomic）
	// ======== 锁保护的索引 ========
	mu syncutil.Mutex
	// lru的作用：
	// 1. 追踪访问顺序
	// 2. O(1)移动到头部
	// 3. O(1)获取尾部（eviction）
	lru partitionList // 双向循环链表
	// parts的作用：
	// 1. O(1)查找partition
	// 2. 判断Range是否在缓存中
	// 3. Eviction时删除

	parts map[roachpb.RangeID]*partition
}

// Design
//
// Cache is designed to be a shared store-wide object which incurs low
// contention for operations on different ranges while maintaining a global
// memory policy. This is achieved through the use of a two-level locking scheme.
// Cache.mu is acquired to access any data in the cache (Add, Clear, Get, or
// Scan) in order to locate the partition for the operation and update the LRU
// state. In the case of Add operations, partitions are lazily constructed
// under the lock. In addition to partition location, Add operations record the
// maximal amount of space that the write may add to the cache, accepting that
// in certain cases, less space may actually be consumed leading to unnecessary
// evictions. Once a partition has been located (or not found) and LRU state has
// been appropriately modified, operations release Cache.mu and proceed by
// operating on the partition under its RWMutex.
//
// This disjoint, two-level locking pattern permits the "anomaly" whereby a
// partition may be accessed and evicted concurrently. This condition is made
// safe in the implementation by using atomics to update the cache bookkeeping
// and by taking care to not mutate the partition's cache state upon eviction.
// As noted above, the Cache and partition's bookkeeping is updated with an
// initial estimate of the byte size of an addition while holding Cache.mu.
// Because empty additions are elided, this initial bookkeeping guarantees that
// the cacheSize of partition is non-zero while an Add operation proceeds unless
// the partition has been evicted. The updated value of partition.size is
// recorded before releasing Cache.mu. When a partition mutation operation
// concludes the Cache's stats need to be updated such that they reflect the new
// reality. This update (Cache.recordUpdate) is mediated through the use of an
// atomic compare and swap operation on partition.size. If the operation
// succeeds, then we know that future evictions of this partition will see the
// new updated partition.size and so any delta from what was optimistically
// recorded in the Cache stats should be updated (using atomics, see
// add(Bytes|Entries)). If the operation fails, then we know that any change
// just made to the partition are no longer stored in the cache and thus the
// Cache stats shall not change.
//
// This approach admits several undesirable conditions, fortunately they aren't
// practical concerns.
//
//   1) Evicted partitions are reclaimed asynchronously only after operations
//      concurrent with evictions complete.
//   2) Memory reuse with object pools is difficult.

type partition struct {
	id roachpb.RangeID // Range标识
	// ======== 数据存储 ========
	mu syncutil.RWMutex
	// 为什么用ringBuf而不是map？
	// 1. entries通常是连续的index
	// 2. ringBuf内存紧凑，缓存友好
	// 3. 支持高效的范围扫描
	// 4. 自动处理index重叠和gap
	ringBuf // implements rangeCache, embedded to avoid interface and allocation// 环形缓冲区
	// ======== 原子大小统计 ========
	// 编码：高32位=entries数，低32位=bytes数
	// 为什么不用两个独立的atomic？
	// - 单次CAS可以同时更新两个值
	// - 避免中间状态（bytes变了但entries没变）
	// - 性能更好
	size cacheSize // atomic uint64
	// ======== LRU链表指针 ========
	// 访问规则：
	// - 读写需要Cache.mu
	// - 嵌入式链表，无额外分配
	next, prev *partition // accessed under Cache.mu
}

// 约48字节（在64位系统上）
// 用于计算partition本身的内存开销
const partitionSize = int32(unsafe.Sizeof(partition{}))

// rangeCache represents the interface that the partition uses.
// It is never explicitly used but a new implementation to replace ringBuf must
// implement the below interface.
type rangeCache interface {
	add(ent []raftpb.Entry) (bytesAdded, entriesAdded int32)
	truncateFrom(lo kvpb.RaftIndex) (bytesRemoved, entriesRemoved int32)
	clearTo(hi kvpb.RaftIndex) (bytesRemoved, entriesRemoved int32)
	get(index kvpb.RaftIndex) (raftpb.Entry, bool)
	scan(ents []raftpb.Entry, lo, hi kvpb.RaftIndex, maxBytes uint64) (
		_ []raftpb.Entry, bytes uint64, nextIdx kvpb.RaftIndex, exceededMaxBytes bool)
}

// ringBuf implements rangeCache.
var _ rangeCache = (*ringBuf)(nil)

// NewCache creates a cache with a max size.
// Size must be less than math.MaxInt32.
func NewCache(maxBytes uint64) *Cache {
	if maxBytes > math.MaxInt32 {
		maxBytes = math.MaxInt32
	}
	return &Cache{
		maxBytes: int32(maxBytes),
		metrics:  makeMetrics(),
		parts:    map[roachpb.RangeID]*partition{},
	}
}

// Metrics returns a struct which contains metrics for the raft entry cache.
func (c *Cache) Metrics() Metrics {
	return c.metrics
}

// Drop drops all cached entries associated with the specified range.
func (c *Cache) Drop(id roachpb.RangeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.getPartLocked(id, false /* create */, false /* recordUse */)
	if p != nil {
		c.updateGauges(c.evictPartitionLocked(p))
	}
}

// Add inserts ents into the cache. If truncate is true, the method also removes
// all entries with indices equal to or greater than the indices of the entries
// provided. ents is expected to consist of entries with a contiguous sequence
// of indices.
func (c *Cache) Add(id roachpb.RangeID, ents []raftpb.Entry, truncate bool) {
	// ======== 阶段0：快速返回 ========
	if len(ents) == 0 {
		return
	}
	// ======== 阶段1：分析entries ========
	bytesGuessed := analyzeEntries(ents)
	// analyzeEntries做什么？
	// 1. 计算总大小：sum(e.Size())
	// 2. 验证连续性：e[i].Index = e[i-1].Index + 1
	// 3. 验证term单调：e[i].Term >= e[i-1].Term
	add := bytesGuessed <= c.maxBytes
	// 如果entries太大（超过整个cache），不添加
	// 例如：ents总大小200MB，cache只有128MB
	if !add {
		bytesGuessed = 0 // 标记为不添加
	}
	// ======== 阶段2：定位或创建partition ========
	c.mu.Lock()
	// Get p and move the partition to the front of the LRU.
	p := c.getPartLocked(id, add /* create */, true /* recordUse */)
	// getPartLocked逻辑：
	// - 查找parts[id]
	// - 如果不存在且create=true，创建新partition
	// - 如果recordUse=true，移动到LRU头部
	if bytesGuessed > 0 {
		// 3.1 先evict以腾出空间
		c.evictLocked(bytesGuessed)
		// evictLocked会：
		// - 从LRU尾部开始evict
		// - 直到bytes + bytesGuessed <= maxBytes

		// 3.2 如果evict了所有partition（极端情况）
		if len(c.parts) == 0 { // Get p again if we evicted everything.
			// 重新创建当前Range的partition
			p = c.getPartLocked(id, true /* create */, false /* recordUse */)
		}
		// Use the atomic (load|set)Size partition methods to avoid a race condition
		// on p.size and to ensure that p.size.bytes() reflects the number of bytes
		// in c.bytes associated with p in the face of concurrent updates due to calls
		// to c.recordUpdate.
		// 3.3 乐观地增加partition.size（猜测值）
		for {
			prev := p.loadSize()
			if p.setSize(prev, prev.add(bytesGuessed, 0)) {
				break
			}
			// CAS失败，重试
		}
	}
	c.mu.Unlock() // ← 关键：Cache.mu已释放！

	// ======== 阶段4：检查partition ========
	if p == nil {
		// partition不存在且没有创建
		// 只有在!add时可能发生
		// The partition did not exist and we did not create it.
		// Only possible if !add.
		return
	}
	// ======== 阶段5：修改partition数据 ========
	p.mu.Lock()
	defer p.mu.Unlock()
	var bytesAdded, entriesAdded, bytesRemoved, entriesRemoved int32
	// We truncate (if requested) before adding. This can lead to "wasted"
	// work where we're zeroing out entries we would repopulate anyway, but
	// past experience shows that it's best to keep this code as simple as
	// possible (see #61990).
	// 5.1 可选：先截断
	if truncate {
		// 删除index >= ents[0].Index的所有旧entries
		// Note that ents[0].Index may not even be in the cache
		// at this point. `truncateFrom` will still remove any entries
		// it may have at indexes >= truncIdx, as instructed.
		// 为什么需要truncate？
		// - Leader改变时，新Leader可能覆盖旧entries
		// - 例如：旧[100,101,102]，新[100,101,103]
		// - 必须先删除102
		truncIdx := kvpb.RaftIndex(ents[0].Index)
		bytesRemoved, entriesRemoved = p.truncateFrom(truncIdx)
	}
	// 5.2 添加新entries
	if add {
		bytesAdded, entriesAdded = p.add(ents)
		// ringBuf.add会智能处理：
		// - 连续追加
		// - 重叠覆盖
		// - 扩展buf
	}
	// ======== 阶段6：更新统计 ========
	c.recordUpdate(p, bytesAdded-bytesRemoved /*实际字节变化*/, bytesGuessed /*之前猜测的字节*/, entriesAdded-entriesRemoved)
}

// Clear removes all entries on the given range with index <= hi.
func (c *Cache) Clear(id roachpb.RangeID, hi kvpb.RaftIndex) {
	// ======== 阶段1：定位partition ========
	c.mu.Lock()
	// 注意：
	// - create=false：Clear不创建partition
	// - recordUse=false：Clear不算"使用"（避免影响LRU）
	p := c.getPartLocked(id, false /* create */, false /* recordUse */)
	if p == nil {
		c.mu.Unlock()
		return // 该Range不在缓存中，无需清理
	}
	c.mu.Unlock()
	// ======== 阶段2：清理数据 ========
	p.mu.Lock()
	defer p.mu.Unlock()
	// ringBuf.clearTo逻辑：
	// 1. 从head开始遍历
	// 2. 清零所有index <= hi的entries
	// 3. 移动head指针
	// 4. 可能触发buf收缩
	bytesRemoved, entriesRemoved := p.clearTo(hi)
	// ======== 阶段3：更新统计 ========
	c.recordUpdate(p, -1*bytesRemoved, 0, -1*entriesRemoved)
}

// Get returns the entry for the specified index and true for the second return
// value. If the index is not present in the cache, false is returned.
// Get操作的锁序列
func (c *Cache) Get(id roachpb.RangeID, idx kvpb.RaftIndex) (e raftpb.Entry, ok bool) {
	c.metrics.Accesses.Inc(1)
	// 阶段1：快速定位partition
	c.mu.Lock()
	// 注意：
	// - create=false：不创建新partition（Get不应该修改cache）
	// - recordUse=true：更新LRU（标记为最近使用）
	p := c.getPartLocked(id, false /* create */, true /* recordUse */)
	c.mu.Unlock()
	if p == nil {
		// Cache miss：该Range不在缓存中
		return e, false
	}
	p.mu.RLock() // ← 只锁这个Range
	defer p.mu.RUnlock()
	// 阶段2：访问partition数据
	// ringBuf.get逻辑：
	// - 计算offset = idx - firstIndex
	// - 检查offset是否在[0, len)范围
	// - 返回buf[(head+offset) % len(buf)]
	e, ok = p.get(idx)
	if ok {
		// ======== 阶段4：更新指标 ========
		c.metrics.Hits.Inc(1)
		c.metrics.ReadBytes.Inc(int64(e.Size()))
	}
	return e, ok
}

// Scan returns entries between [lo, hi) for specified range. If any entries are
// returned for the specified indices, they will start with index lo and proceed
// sequentially without gaps until 1) all entries exclusive of hi are fetched,
// 2) fetching another entry would add up to more than maxBytes of data, or 3) a
// cache miss occurs. The returned size reflects the size of the returned
// entries.
func (c *Cache) Scan(
	ents []raftpb.Entry, id roachpb.RangeID, lo, hi kvpb.RaftIndex, maxBytes uint64,
) (_ []raftpb.Entry, bytes uint64, nextIdx kvpb.RaftIndex, exceededMaxBytes bool) {
	// ======== 阶段1：记录访问 ========
	c.metrics.Accesses.Inc(1)
	// ======== 阶段2：定位partition ========
	c.mu.Lock()
	p := c.getPartLocked(id, false /* create */, true /* recordUse */)
	c.mu.Unlock()
	if p == nil {
		// 未命中：返回空结果
		return ents, 0, lo, false
	}
	// ======== 阶段3：扫描数据 ========
	p.mu.RLock()
	defer p.mu.RUnlock()
	// ringBuf.scan的逻辑：
	// 1. 从lo开始迭代
	// 2. 累加bytes，检查是否超过maxBytes
	// 3. 遇到gap或hi停止
	// 4. 返回读取到的entries和停止位置

	ents, bytes, nextIdx, exceededMaxBytes = p.scan(ents, lo, hi, maxBytes)
	// Track all bytes that are returned to caller, but only consider an access a
	// "hit" if it returns all requested entries or stops short because of a
	// maximum bytes limit.
	// ======== 阶段4：更新指标 ========
	c.metrics.ReadBytes.Inc(int64(bytes))
	// 判断是否算"命中"
	// 命中条件：读取了所有请求的entries OR 因maxBytes停止
	if nextIdx == hi || exceededMaxBytes {
		c.metrics.Hits.Inc(1)
	}
	// 如果因为cache miss停止（nextIdx < hi && !exceededMaxBytes）
	// 不算命中
	return ents, bytes, nextIdx, exceededMaxBytes
}

func (c *Cache) getPartLocked(id roachpb.RangeID, create, recordUse bool) *partition {
	part := c.parts[id]
	if create && part == nil {
		part = c.lru.pushFront(id)
		c.parts[id] = part
		c.addBytes(partitionSize)
	}
	if recordUse && part != nil {
		c.lru.moveToFront(part) // 更新LRU，放到队头
	}
	return part
}

// evictLocked adds toAdd to the current cache byte size and evicts partitions
// until the cache is below the maxBytes threshold. toAdd must be smaller than
// c.maxBytes.
func (c *Cache) evictLocked(toAdd int32) {
	bytes := c.addBytes(toAdd)
	for bytes > c.maxBytes && len(c.parts) > 0 {
		bytes, _ = c.evictPartitionLocked(c.lru.back())
	}
}

func (c *Cache) evictPartitionLocked(p *partition) (updatedBytes, updatedEntries int32) {
	delete(c.parts, p.id)
	c.lru.remove(p)
	pBytes, pEntries := p.evict()
	return c.addBytes(-1 * pBytes), c.addEntries(-1 * pEntries)
}

// recordUpdate adjusts the partition and cache bookkeeping to account for the
// changes which actually occurred in an update relative to the guess made
// before the update.
func (c *Cache) recordUpdate(
	p *partition,
	bytesAdded, // 实际添加的字节（可能是负数，如Clear）
	bytesGuessed, // 之前猜测的字节
	entriesAdded int32) { // 实际添加的entry数
	// This method is always called while p.mu is held.
	// The below code takes care to ensure that all bytes in c due to p are
	// updated appropriately.

	// NB: The loop and atomics are used because p.size can be modified
	// concurrently to calls to recordUpdate. In all cases where p.size is updated
	// outside of this function occur while c.mu is held inside of c.Add. These
	// occur when either:
	//
	//   1) a new write adds its guessed write size to p
	//   2) p is evicted to make room for a write
	//
	// Thus p.size is either increasing or becomes evicted while we attempt to
	// record the update to p. Once p is evicted it stays evicted forever.
	// These facts combine to ensure that p.size never becomes negative from the
	// below call to add.
	// 前置条件：调用者持有p.mu（写锁）
	// 这保证partition数据不会被并发修改

	// ======== 核心算法：CAS循环 ========
	delta := bytesAdded - bytesGuessed
	// delta可能是：
	// - 正数：实际大于预估（少见）
	// - 负数：实际小于预估（常见）
	// - 零：预估准确（理想情况）
	for {
		// 步骤1：原子读取当前size
		curSize := p.loadSize()
		// loadSize = atomic.LoadUint64(&p.size)

		// 步骤2：检查是否已被evict
		if curSize == evicted {
			// partition已经被evict！
			// 我们的修改不应该影响Cache统计
			return
		}
		// 步骤3：计算新的size
		// add方法：
		// - 解码curSize（bytes, entries）
		// - 加上delta和entriesAdded
		// - 重新编码为uint64
		newSize := curSize.add(delta, entriesAdded)
		// 步骤4：CAS更新
		if updated := p.setSize(curSize, newSize); updated {
			// CAS成功！我们成功更新了partition.size
			// 现在更新Cache的全局统计
			c.updateGauges(c.addBytes(delta), c.addEntries(entriesAdded))
			return
		}
		// CAS失败，可能的原因：
		// 1. 另一个Add操作同时修改了p.size
		// 2. partition被evict了
		// 重试...
	}
}

func (c *Cache) addBytes(toAdd int32) int32 {
	return atomic.AddInt32(&c.bytes, toAdd)
}

func (c *Cache) addEntries(toAdd int32) int32 {
	return atomic.AddInt32(&c.entries, toAdd)
}

func (c *Cache) updateGauges(bytes, entries int32) {
	c.metrics.Bytes.Update(int64(bytes))
	c.metrics.Size.Update(int64(entries))
}

var initialSize = newCacheSize(partitionSize, 0)

func newPartition(id roachpb.RangeID) *partition {
	return &partition{
		id:   id,
		size: initialSize,
	}
}

// 特殊值
const evicted cacheSize = 0 // 表示已被evict

func (p *partition) evict() (bytes, entries int32) {
	// Atomically setting size to evicted signals that the partition has been
	// evicted. Changes to p which happen concurrently with the eviction should
	// not be reflected in the Cache. The loop in recordUpdate detects the action
	// of this call.
	cs := p.loadSize()
	for !p.setSize(cs, evicted) {
		cs = p.loadSize()
	}
	return cs.bytes(), cs.entries()
}

func (p *partition) loadSize() cacheSize {
	return cacheSize(atomic.LoadUint64((*uint64)(&p.size)))
}

// 使用CAS原子操作
func (p *partition) setSize(orig, new cacheSize) bool {
	return atomic.CompareAndSwapUint64((*uint64)(&p.size), uint64(orig), uint64(new))
}

// analyzeEntries calculates the size in bytes of ents and ensures that the
// entries in ents have contiguous indices.
func analyzeEntries(ents []raftpb.Entry) (size int32) {
	var prevIndex uint64
	var prevTerm uint64
	for i, e := range ents {
		if i != 0 && e.Index != prevIndex+1 {
			panic(errors.AssertionFailedf("invalid non-contiguous set of entries %d and %d", prevIndex, e.Index))
		}
		if i != 0 && e.Term < prevTerm {
			err := errors.AssertionFailedf("term regression idx %d: %d -> %d", prevIndex, prevTerm, e.Term)
			panic(err)
		}
		prevIndex = e.Index
		prevTerm = e.Term
		size += int32(e.Size())
	}
	return
}

// cacheSize stores int32 counters for numbers of bytes and entries in a single
// 64-bit word.
// 布局：
// ┌────────────────────┬────────────────────┐
// │   高32位: entries  │   低32位: bytes     │
// └────────────────────┴────────────────────┘
type cacheSize uint64

func newCacheSize(bytes, entries int32) cacheSize {
	return cacheSize((uint64(entries) << 32) | uint64(bytes))
}

func (cs cacheSize) entries() int32 {
	return int32(cs >> 32)
}

func (cs cacheSize) bytes() int32 {
	return int32(cs & math.MaxUint32)
}

// add constructs a new cacheSize with signed additions to entries and bytes.
// It is illegal to use values that will make cs negative.
func (cs cacheSize) add(bytes, entries int32) cacheSize {
	return newCacheSize(cs.bytes()+bytes, cs.entries()+entries)
}

// entryList is a double-linked circular list of *partition elements. The code
// is derived from the stdlib container/list but customized to partition in
// order to avoid a separate allocation for every element.
type partitionList struct {
	root partition
}

func (l *partitionList) lazyInit() {
	if l.root.next == nil {
		l.root.next = &l.root
		l.root.prev = &l.root
	}
}

func (l *partitionList) pushFront(id roachpb.RangeID) *partition {
	l.lazyInit()
	return l.insert(newPartition(id), &l.root)
}

func (l *partitionList) moveToFront(p *partition) {
	l.insert(l.remove(p), &l.root)
}

func (l *partitionList) insert(e, at *partition) *partition {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	return e
}

func (l *partitionList) back() *partition {
	if l.root.prev == nil || l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

func (l *partitionList) remove(e *partition) *partition {
	if e == &l.root {
		panic("cannot remove root list node")
	}
	if e.next != nil {
		e.prev.next = e.next
		e.next.prev = e.prev
		e.next = nil // avoid memory leaks
		e.prev = nil // avoid memory leaks
	}
	return e
}
