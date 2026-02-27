// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftentry

import (
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// ringBuf is a ring buffer of raft entries.
// **为什么使用环形缓冲区？**
// ```
// Raft log的特点：
// 1. Index连续：[100, 101, 102, 103, ...]
// 2. 顺序追加：新entries追加到尾部
// 3. 从头清理：旧entries从头部删除
// 4. 范围查询：Scan [lo, hi)
//
// 环形缓冲区完美匹配这些特点！
// ```
type ringBuf struct {
	buf  []raftpb.Entry // 底层数组（容量是2的幂）
	head int            // 第一个有效entry的位置
	len  int            // 有效entry数量
}

const (
	shrinkThreshold = 8  // shrink buf if len(buf)/len is above this.
	minBufSize      = 16 /* entries */
)

// add adds ents to the ringBuf keeping track of how much was actually added
// given that ents may overlap with existing entries or may be rejected from
// the buffer. ents must not be empty.
// 1. 添加entries
func (b *ringBuf) add(ents []raftpb.Entry) (addedBytes, addedEntries int32) {
	// 步骤1：检查是否需要扩展
	// - 如果ents是连续的（紧接着最后一个entry）
	// - 如果buf空间不足，扩展buf（realloc）

	// 步骤2：处理index重叠
	// - 如果ents[0].Index <= lastCachedIndex
	// - 覆盖重叠的部分

	// 步骤3：写入新entries
	// - 使用iterator遍历
	// - 处理环形边界
	if it := last(b); it.valid(b) && kvpb.RaftIndex(ents[0].Index) > it.index(b)+1 {
		// 检测gap：如果新entries不连续且更新
		// 例如：
		// 当前：[3, 4, 5]，last=5
		// 添加：[8, 9]，first=8
		// gap：8 > 5+1，不连续
		// If ents is non-contiguous and later than the currently cached range then
		// remove the current entries and add ents in their place.
		// 清除当前所有entries
		removedBytes, removedEntries := b.clearTo(it.index(b))
		addedBytes, addedEntries = -1*removedBytes, -1*removedEntries
	}
	// 计算重叠部分
	before, after, ok := computeExtension(b, kvpb.RaftIndex(ents[0].Index), kvpb.RaftIndex(ents[len(ents)-1].Index))
	if !ok {
		return
	}
	extend(b, before, after)
	it := first(b)
	if before == 0 && after != b.len { // skip unchanged prefix
		it, _ = iterateFrom(b, kvpb.RaftIndex(ents[0].Index)) // safe by construction
	}
	firstNewAfter := len(ents) - after
	// before: 在当前范围之前的新entries数量
	// after: 在当前范围之后的新entries数量
	// 中间部分：重叠，需要覆盖

	// 覆盖写入
	for i, e := range ents {
		if i < before || i >= firstNewAfter {
			// 新增的entry
			addedEntries++
			addedBytes += int32(e.Size())
		} else {
			// 覆盖的entry
			addedBytes += int32(e.Size() - it.entry(b).Size())
			// 注意：可能变大或变小
		}
		it = it.push(b, e)
	}
	return
}

// truncateFrom clears all entries from the ringBuf with index equal to or
// greater than lo. The method returns the aggregate size and count of entries
// removed. Note that lo itself may or may not be in the cache.
func (b *ringBuf) truncateFrom(lo kvpb.RaftIndex) (removedBytes, removedEntries int32) {
	if b.len == 0 {
		return
	}
	if idx := first(b).index(b); idx > lo {
		// If `lo` precedes the indexes in the buffer
		// (say the buf is idx=[100, 101, 102] and `lo` is 99),
		// `iterateFrom` will return an invalid iter. But we
		// need to truncate everything and so advance to the
		// first index before constructing the iterator.
		lo = idx
	}
	it, ok := iterateFrom(b, lo)
	for ok {
		removedBytes += int32(it.entry(b).Size())
		removedEntries++
		it.clear(b)
		it, ok = it.next(b)
	}
	b.len -= int(removedEntries)
	//收缩策略：
	//- 当使用率 < 12.5% 时收缩
	//- 避免频繁收缩：阈值是8
	//
	//示例1：
	//buf容量 = 128
	//len = 15 (< 128/8 = 16)
	//→ 收缩到32（大于等于15的最小2的幂）
	//
	//示例2：
	//buf容量 = 64
	//len = 10 (> 64/8 = 8)
	//→ 不收缩
	//
	//为什么是8？
	//- 太小（如2）：频繁收缩，性能差
	//- 太大（如16）：内存浪费多
	//- 8是经验值：平衡性能和内存
	if b.len < (len(b.buf) / shrinkThreshold) {
		// 在clearTo()和truncateFrom()中
		realloc(b, 0, b.len)
	}
	if util.RaceEnabled {
		if b.len > 0 {
			if lastIdx := last(b).index(b); lastIdx >= lo {
				panic(errors.AssertionFailedf(
					"buffer truncated to [..., %d], but current last index is %d",
					lo, lastIdx,
				))
			}
		}
	}
	return removedBytes, removedEntries
}

// clearTo clears all entries from the ringBuf with index <= hi. The
// method returns the aggregate size and count of entries removed.
func (b *ringBuf) clearTo(hi kvpb.RaftIndex) (removedBytes, removedEntries int32) {
	if b.len == 0 || hi < first(b).index(b) {
		return
	}
	it := first(b)
	ok := it.valid(b) // true
	for ok && it.index(b) <= hi {
		removedBytes += int32(it.entry(b).Size())
		removedEntries++
		it.clear(b)
		it, ok = it.next(b)
	}
	offset := int(removedEntries)
	b.len -= offset
	b.head = (b.head + offset) % len(b.buf)
	if b.len < (len(b.buf) / shrinkThreshold) {
		realloc(b, 0, b.len)
	}
	return
}

func (b *ringBuf) get(index kvpb.RaftIndex) (e raftpb.Entry, ok bool) {
	it, ok := iterateFrom(b, index)
	if !ok {
		return e, ok
	}
	return *it.entry(b), ok
}

func (b *ringBuf) scan(
	ents []raftpb.Entry, lo kvpb.RaftIndex, hi kvpb.RaftIndex, maxBytes uint64,
) (_ []raftpb.Entry, bytes uint64, nextIdx kvpb.RaftIndex, exceededMaxBytes bool) {
	var it iterator
	nextIdx = lo
	it, ok := iterateFrom(b, lo)
	for ok && !exceededMaxBytes && it.index(b) < hi {
		e := it.entry(b)
		s := uint64(e.Size())
		exceededMaxBytes = bytes+s > maxBytes
		if exceededMaxBytes && len(ents) > 0 {
			break
		}
		bytes += s
		ents = append(ents, *e)
		nextIdx++
		it, ok = it.next(b)
	}
	return ents, bytes, nextIdx, exceededMaxBytes
}

// reallocs b.buf into a new buffer of newSize leaving before zero value entries
// at the front of b.
func realloc(b *ringBuf, before, newLen int) {
	newBuf := make([]raftpb.Entry, reallocLen(newLen))
	if b.head+b.len > len(b.buf) {
		n := copy(newBuf[before:], b.buf[b.head:])
		copy(newBuf[before+n:], b.buf[:(b.head+b.len)%len(b.buf)])
	} else {
		copy(newBuf[before:], b.buf[b.head:b.head+b.len])
	}
	b.buf = newBuf
	b.head = 0
	b.len = newLen
}

// reallocLen returns a new length which is a power-of-two greater than or equal
// to need and at least minBufSize.
// 示例：
// reallocLen(10)  = 16    (2^4)
// reallocLen(20)  = 32    (2^5)
// reallocLen(50)  = 64    (2^6)
// reallocLen(100) = 128   (2^7)
// bits.Len()的工作原理：
// bits.Len(50) = 6  // 50 = 0b110010, 需要6位
// 1 << 6 = 64       // 2^6
//
// 保证：
// 1. 容量始终是2的幂
// 2. 容量 >= need
// 3. 容量 >= minBufSize (16)
func reallocLen(need int) (newLen int) {
	if need <= minBufSize {
		return minBufSize
	}
	// bits.Len(n) 返回表示n需要的位数
	// 1 << bits.Len(n) 返回大于等于n的最小的2的幂
	return 1 << uint(bits.Len(uint(need)))
}

// extend takes a number of entries before and after the current cached values
// to increase the length of b. The before-length prefix of b will now be zero
// valued entries.
// 示例：
// 当前：buf=[16个entry], len=16, 满了
// 添加：10个新entry
// 需要：16 + 10 = 26
// 扩容到：32（下一个2的幂）
func extend(b *ringBuf, before, after int) {
	size := before + b.len + after
	if size > len(b.buf) {
		//需要扩容
		realloc(b, before, size)
	} else {
		b.head = (b.head - before) % len(b.buf)
		if b.head < 0 {
			b.head += len(b.buf)
		}
	}
	b.len = size
}

// computeExtension returns the number of entries in [lo, hi] which will be
// added before and after the current range of the cache. Note that lo and hi
// here are inclusive indices for the range being added and that before and
// after are counts, not indices, of number of entries which precede and follow
// the currently cached range. If [lo, hi] is not overlapping or directly
// adjacent to the current cache bounds, ok will be false.
func computeExtension(b *ringBuf, lo, hi kvpb.RaftIndex) (before, after int, ok bool) {
	if b.len == 0 {
		return 0, int(hi) - int(lo) + 1, true
	}
	first, last := first(b).index(b), last(b).index(b)
	if lo > (last+1) || hi < (first-1) { // gap case
		return 0, 0, false
	}
	if lo < first {
		before = int(first) - int(lo)
	}
	if hi > last {
		after = int(hi) - int(last)
	}
	return before, after, true
}

// iterator indexes into a ringBuf. A value of -1 is not valid.
type iterator int

func iterateFrom(b *ringBuf, index kvpb.RaftIndex) (_ iterator, ok bool) {
	if b.len == 0 {
		return -1, false
	}
	offset := int(index) - int(first(b).index(b))
	if offset < 0 || offset >= b.len {
		return -1, false
	}
	return iterator((b.head + offset) % len(b.buf)), true
}

// first returns an iterator pointing to the first entry of the ringBuf.
// If b is empty, the returned iterator is not valid.
func first(b *ringBuf) iterator {
	if b.len == 0 {
		return iterator(-1)
	}
	return iterator(b.head)
}

// last returns an iterator pointing to the last element in b.
// If b is empty, the returned iterator is not valid.
func last(b *ringBuf) iterator {
	if b.len == 0 {
		return iterator(-1)
	}
	return iterator((b.head + b.len - 1) % len(b.buf))
}

func (it iterator) valid(b *ringBuf) bool {
	return it >= 0 && int(it) < len(b.buf)
}

// index returns the index of the entry at iterator's curent position.
func (it iterator) index(b *ringBuf) kvpb.RaftIndex {
	return kvpb.RaftIndex(b.buf[it].Index)
}

// entry returns the entry at iterator's curent position.
func (it iterator) entry(b *ringBuf) *raftpb.Entry {
	return &b.buf[it]
}

// clear zeroes the current value in b.
func (it iterator) clear(b *ringBuf) {
	b.buf[it] = raftpb.Entry{}
}

// next returns an iterator which points to the next element in b.
// If it is invalid or points to the last element in b, (-1, false) is returned.
func (it iterator) next(b *ringBuf) (_ iterator, ok bool) {
	if !it.valid(b) || it == last(b) {
		return -1, false
	}
	return iterator(int(it+1) % len(b.buf)), true
}

// push sets the iterator's current value in b to e and calls next
// It is the caller's responsibility to ensure that b has space for the new
// entry.
func (it iterator) push(b *ringBuf, e raftpb.Entry) iterator {
	b.buf[it] = e
	it, _ = it.next(b)
	return it
}
