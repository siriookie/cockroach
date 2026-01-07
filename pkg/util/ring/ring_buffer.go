// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ring

// Buffer is a deque maintained over a ring buffer.
//
// The zero value is ready to use. See MakeBuffer() for initializing a Buffer
// with pre-allocated space.
//
// Note: it is backed by a slice (unlike container/ring/ring_buffer.go which
// is backed by a linked list). There is also a container/ring/buffer.go, that
// is backed by a slice and can both grow and shrink and uses bit arithmetic.
// We should replace this implementation with that one.
type Buffer[T any] struct {
	buffer []T
	head   int // the index of the front of the buffer
	tail   int // the index of the first position after the end of the buffer

	// Indicates whether the buffer is empty. Necessary to distinguish
	// between an empty buffer and a buffer that uses all of its capacity.
	nonEmpty bool
}

// MakeBuffer creates a buffer.
//
// scratch, if not nil, represents pre-allocated space that the Buffer takes
// ownership of. The whole backing array of the provided slice is taken over,
// included elements and available capacity.
func MakeBuffer[T any](scratch []T) Buffer[T] {
	return Buffer[T]{buffer: scratch}
}

// Len returns the number of elements in the Buffer.
func (r *Buffer[T]) Len() int {
	if !r.nonEmpty {
		return 0
	}
	if r.head < r.tail {
		return r.tail - r.head
	} else if r.head == r.tail {
		return cap(r.buffer)
	} else {
		return cap(r.buffer) + r.tail - r.head
	}
}

// Cap returns the capacity of the Buffer.
func (r *Buffer[T]) Cap() int {
	return cap(r.buffer)
}

// Get returns an element at position pos in the Buffer (zero-based).
func (r *Buffer[T]) Get(pos int) T {
	if !r.nonEmpty || pos < 0 || pos >= r.Len() {
		panic("index out of bounds")
	}
	return r.buffer[(pos+r.head)%cap(r.buffer)]
}

// GetFirst returns an element at the front of the Buffer.
func (r *Buffer[T]) GetFirst() T {
	if !r.nonEmpty {
		panic("getting first from empty ring buffer")
	}
	return r.buffer[r.head]
}

// GetLast returns an element at the front of the Buffer.
//例子 1：最普通情况（不绕圈）
//状态
//buffer: [ A B C _ _ ]
//index:    0 1 2 3 4
//tail = 3   // 下一个要写的位置
//最后一个元素是谁？
//👉 是 C，在 index 2
//套公式
//(cap + tail - 1) % cap
//= (5 + 3 - 1) % 5
//= 7 % 5
//= 2
//✔️ 正好是 C
//例子 2：发生“环绕”（关键例子）
//状态
//buffer: [ D E A B C ]
//index:    0 1 2 3 4
//tail = 2   // 下一个要写到 index 2
//说明发生过环绕，逻辑顺序是：
//A → B → C → D → E
//最后一个元素是 E，在 index 1
//套公式
//(cap + tail - 1) % cap
//= (5 + 2 - 1) % 5
//= 6 % 5
//= 1
//✔️ 正好拿到 E
//例子 3：最容易犯错的边界（tail == 0）
//状态
//buffer: [ B C D E A ]
//index:    0 1 2 3 4
//tail = 0   // 下一个写到 0

//最后一个元素是 A，在 index 4
//如果你天真写：
//tail - 1 = -1 ❌ 越界
//用这个公式：
//(cap + tail - 1) % cap
//= (5 + 0 - 1) % 5
//= 4 % 5
//= 4

// ✔️ 自动绕回最后一个位置
func (r *Buffer[T]) GetLast() T {
	if !r.nonEmpty {
		panic("getting last from empty ring buffer")
	}
	return r.buffer[(cap(r.buffer)+r.tail-1)%cap(r.buffer)]
}

// `时间轴（每 100ms 采样一次）：
// t0    t1    t2    t3    ... t24   t25
// │─────│─────│─────│─────│─...─│─────│
// [H0]  [H1]  [H2]  [H3]       [H24] [H25]
//
// 环形缓冲区（容量 = sampleDuration / samplePeriod = 25）：
// ┌────┬────┬────┬────┬────┬────┬────┐
// │ H1 │ H2 │ H3 │... │H24 │H25 │    │  ← 最新样本在头部
// └────┴────┴────┴────┴────┴────┴────┘
//
//	↑                           ↑
//
// newest                     oldest
//
// P99 计算：P99 = percentile(H25 - H1, 0.99)`
func (r *Buffer[T]) resize(n int) {
	if n < r.Len() {
		panic("resizing to fewer elements than current length")
	}

	if n == 0 {
		r.Discard()
		return
	}

	newBuffer := make([]T, n)
	r.copyTo(newBuffer)
	r.tail = r.Len() % cap(newBuffer)
	r.head = 0
	r.buffer = newBuffer
}

// copyTo copies elements from r to dst. If len(dst) < r.Len(), only the first
// len(dst) elements are copied.
func (r *Buffer[T]) copyTo(dst []T) {
	if !r.nonEmpty {
		return
	}
	// Copy over the contents to dst.
	if r.head < r.tail {
		copy(dst, r.buffer[r.head:r.tail])
	} else {
		tailElements := r.buffer[r.head:]
		copy(dst, tailElements)
		// If there's space remaining, continue.
		if len(dst) > len(tailElements) {
			copy(dst[cap(r.buffer)-r.head:], r.buffer[:r.tail])
		}
	}
}

func (r *Buffer[T]) maybeGrow() {
	if r.Len() != cap(r.buffer) {
		return
	}
	n := 2 * cap(r.buffer)
	if n == 0 {
		n = 1
	}
	r.resize(n)
}

// AddFirst add element to the front of the Buffer and doubles it's underlying
// slice if necessary.
func (r *Buffer[T]) AddFirst(element T) {
	r.maybeGrow()
	r.head = (cap(r.buffer) + r.head - 1) % cap(r.buffer)
	r.buffer[r.head] = element
	r.nonEmpty = true
}

// AddLast adds element to the end of the Buffer and doubles it's underlying
// slice if necessary.
func (r *Buffer[T]) AddLast(element T) {
	r.maybeGrow()
	r.buffer[r.tail] = element
	r.tail = (r.tail + 1) % cap(r.buffer)
	r.nonEmpty = true
}

// RemoveFirst removes a single element from the front of the Buffer.
func (r *Buffer[T]) RemoveFirst() {
	if r.Len() == 0 {
		panic("removing first from empty ring buffer")
	}
	var zero T
	r.buffer[r.head] = zero
	r.head = (r.head + 1) % cap(r.buffer)
	if r.head == r.tail {
		r.nonEmpty = false
	}
}

// RemoveLast removes a single element from the end of the Buffer.
func (r *Buffer[T]) RemoveLast() {
	if r.Len() == 0 {
		panic("removing last from empty ring buffer")
	}
	lastPos := (cap(r.buffer) + r.tail - 1) % cap(r.buffer)
	var zero T
	r.buffer[lastPos] = zero
	r.tail = lastPos
	if r.tail == r.head {
		r.nonEmpty = false
	}
}

// Reserve reserves the provided number of elements in the Buffer. It is illegal
// to reserve a size less than the r.Len().
//
// If the Buffer already has a capacity of n or larger, this is a no-op.
func (r *Buffer[T]) Reserve(n int) {
	if n < r.Len() {
		panic("reserving fewer elements than current length")
	}
	if n > cap(r.buffer) {
		r.resize(n)
	}
}

// Resize changes the Buffer's storage to be of the specified size. It is
// illegal to resize to a size less than r.Len().
//
// This is a more general version of Reserve: Reserve only ever grows the
// storage, whereas Resize() can also shrink it.
//
// Note that, if n != r.Len(), Resize always allocates new storage, even when n
// is less than the current capacity. This can be useful to make the storage for
// a buffer that used to be large available for GC, but it can also be wasteful.
func (r *Buffer[T]) Resize(n int) {
	if n < r.Len() {
		panic("resizing to fewer elements than current length")
	}

	if n != cap(r.buffer) {
		r.resize(n)
	}
}

// Reset makes Buffer treat its underlying memory as if it were empty. This
// allows for reusing the same memory again without explicitly removing old
// elements. Note that this does not nil out the elements, so they're not made
// available to GC.
//
// See also Discard.
func (r *Buffer[T]) Reset() {
	r.head = 0
	r.tail = 0
	r.nonEmpty = false
}

// Discard is like Reset, except it also does Resize(0) to nil out the
// underlying slice. This makes the backing storage for the slice available to
// GC if nobody else is referencing it. This is useful if r is still referenced,
// but *r will be reassigned.
//
// See also Reset and Resize.
func (r *Buffer[T]) Discard() {
	*r = Buffer[T]{}
}

// all a slice with returns all the elements in the buffer.
func (r *Buffer[T]) all() []T {
	buf := make([]T, r.Len())
	r.copyTo(buf)
	return buf
}
