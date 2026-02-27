// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slidingwindow

import "time"

// Swag represents a sliding window aggregator over a binary operation.
// The aggregator will aggregate recorded values in two ways:
// (1) Within each window, the binary operation is applied when recording a new
//
//	value into the current window.
//
// (2) On Query, the binary operation is accumulated over every window, from
//
//	most to least recent.
//
// The binary operator function must therefore be:
//
//	associative :: binOp(binOp(a,b), c) = binOp(a,binOp(b,c))
//
// In order to have correct results. Note that this does not allow for a more
// general class of aggregators that may be  associative, such as geometric
// mean, bloom filters etc. These require special treatment with user defined
// functions for lift(e), lower(a) and combine(v1,v2)
// (https://dl.acm.org/doi/pdf/10.1145/3093742.3093925).
//
// query: O(k), append: O(k), space O(k), k = |windows|.
// The average case append is O(1), when no windows
// require rotating; the size requirement is 32 + 8k bytes.
// Swag 是一个基于时间分桶的滑动窗口聚合器（Sliding Window Aggregator），支持任意结合律（associative）二元操作。它的核心职责是：
// 输入（构造时）：
// - now：当前时间（用于初始化窗口）
// - interval：每个窗口的时间跨度（如 1 分钟）
// - size：窗口数量（如 5 个窗口）
// - binOp：二元聚合函数（如 max(a, b)、sum(a, b)）
// 输入（运行时）：
// - Record(now, val)：记录一个新值到当前窗口
// - Query(now)：查询所有窗口的聚合结果
// 输出：
// - 聚合值（如过去 5 分钟的最大值）
// - 实际覆盖的时间跨度（如果系统运行不足 5 分钟，返回实际跨度）
// 时间复杂度：
// - Record()：平均 O(1)（无需轮转时），最坏 O(k)（需要轮转 k 个窗口）
// - Query()：O(k)，k = 窗口数量
// 空间复杂度：
// - 32 + 8k 字节（对于 5 个窗口，约 72 字节）
type Swag struct {
	curIdx         int                            //占用 8 字节（64 bits）
	windows        []*float64                     //24字节
	lastRotate     time.Time                      //24 字节
	rotateInterval time.Duration                  //8 字节
	binOp          func(acc, val float64) float64 //8 字节
}

// NewSwag returns a new sliding window aggregator.
func NewSwag(
	now time.Time, interval time.Duration, size int, binOp func(acc, val float64) float64,
) *Swag {
	windows := make([]*float64, size) // 分配 5 个指针位置
	var first float64                 // 初始化第一个窗口为 0
	windows[0] = &first
	return &Swag{
		curIdx:         0,        // 当前窗口索引 = 0
		windows:        windows,  // [&0, nil, nil, nil, nil]
		lastRotate:     now,      // 上次轮转时间 = now
		rotateInterval: interval, // 1 分钟
		binOp:          binOp,    // max 函数
	}
}

// Record takes a value and applies the binary operation with the current
// bucket and the value.
func (s *Swag) Record(now time.Time, val float64) {
	s.maybeRotate(now)                                        // 1. 检查是否需要轮转窗口
	*s.windows[s.curIdx] = s.binOp(*s.windows[s.curIdx], val) // 2. 聚合到当前窗口
}

// maybeRotate checks the passed in time with the last rotate time. If the
// duration elapsed is greater than the rotate interval, it will rotate the
// windows, adding the interval to the last rotate time. This continues until
// the duration elapsed no longer greater last rotate +  interval.
func (s *Swag) maybeRotate(now time.Time) {
	sinceLastRotate := now.Sub(s.lastRotate)
	if sinceLastRotate < s.rotateInterval {
		return
	}

	size := len(s.windows)
	shift := int(sinceLastRotate / s.rotateInterval)
	for i := 0; i < shift; i++ {
		s.curIdx = (s.curIdx + 1) % size
		s.lastRotate = s.lastRotate.Add(s.rotateInterval)
		var next float64
		s.windows[s.curIdx] = &next
	}
}

// Query applies the binOp across each window accumulated, from most recent to
// least recent window. This requires that the binOp fn is associative.
func (s *Swag) Query(now time.Time) (float64, time.Duration) {
	windows := s.Windows(now)
	timeSinceRotate := now.Sub(s.lastRotate)

	var accumulator float64
	var duration time.Duration
	for i, next := range windows {
		accumulator = s.binOp(accumulator, next)
		if i == 0 {
			duration += time.Duration(float64(timeSinceRotate))
		} else {
			duration += time.Duration(float64(s.rotateInterval))
		}
	}
	return accumulator, duration
}

// Windows returns the currently populated windows, in most recent to least
// recent order. It will not return unpopulated windows, if total duration is
// less than size * rotateInterval.
func (s *Swag) Windows(now time.Time) []float64 {
	s.maybeRotate(now) // 确保窗口是最新的
	size := len(s.windows)
	ret := make([]float64, 0, 1)

	for i := 0; i < size; i++ {
		// 从 curIdx 向后遍历（最新到最旧）
		next := s.windows[(s.curIdx+size-i)%size]
		if next == nil {
			break // 遇到未初始化的窗口，停止
		}
		ret = append(ret, *next)
	}
	return ret // 返回 [最新窗口, 次新窗口, ..., 最旧窗口]
}
