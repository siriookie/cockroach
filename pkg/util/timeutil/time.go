// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"strings"
	"time"
	"unsafe"
)

// LibPQTimePrefix is the prefix lib/pq prints time-type datatypes with.
const LibPQTimePrefix = "0000-01-01"

// Now returns the current UTC time.
//
// We've decided in times immemorial that always returning UTC is a good policy
// across the cluster so that all the timestamps print uniformly across
// different nodes, and also because we were afraid that timestamps leak into
// SQL Datums, and there the timestamp matters. Years later, it's not clear
// whether this was a good decision since it's forcing the nasty implementation
// below.
// 我们在**很久很久以前（久远到不记得什么时候）**就已经决定，
// 在整个集群中始终返回 UTC 时间是一个好的策略。这样可以确保所有的时间戳在不同的节点上打印出来都是统一的。
// 另一个原因是，我们担心时间戳会泄露到 SQL 的数据项（Datums）中，而在那里，时区的准确性至关重要。
// 若干年后的今天，我们也不确定当初这个决定是否正确，因为它强迫我们采用了下面这种**非常恶心（nasty）**的底层实现。
func Now() time.Time {
	t := time.Now()
	// HACK: instead of doing t = t.UTC(), we reach inside the
	// struct and set the location manually. UTC() strips the monotonic clock reading
	// from t, for no good reason: https://groups.google.com/g/golang-nuts/c/dyPTdi6oem8
	// Stripping the monotonic part has bad consequences:
	// 1. We lose the benefits of the monotonic clock reading.
	// 2. On OSX, only the monotonic clock seems to have nanosecond resolution. If
	// we strip it, we only get microsecond resolution. Besides generally sucking,
	// microsecond resolution is not enough to guarantee that consecutive
	// timeutil.Now() calls don't return the same instant. This trips up some of
	// our tests, which assume that they can measure any duration of time.
	// 3. time.Since(t) does one less system calls when t has a monotonic reading,
	// making it twice as fast as otherwise:
	// https://cs.opensource.google/go/go/+/refs/tags/go1.17.2:src/time/time.go;l=878;drc=refs%2Ftags%2Fgo1.17.2
	//黑科技（HACK）： 我们没有使用标准的 t = t.UTC()，而是直接“伸手”进结构体内部手动设置时区位置。
	//这是因为 UTC() 函数会无缘无故地从时间对象中**剥离（strip）掉单调时钟（monotonic clock）**的读数。
	//： 剥离掉单调时钟部分会带来严重的后果：
	//失去优势：我们失去了单调时钟读数带来的种种好处。
	//精度灾难（特别是 macOS）：在 macOS 上，似乎只有单调时钟能提供纳秒级的分辨率。如果把它剥离掉，我们就只能得到微秒级的分辨率。除了这本身就很糟糕（sucking）外，微秒级的精度不足以保证连续调用 Now() 不会返回相同的时间点。这会导致我们的一些测试失败，因为那些测试假设它们可以测量任何极短的时间间隔。
	//性能损耗：当 t 包含单调读数时，执行 time.Since(t) 会少进行一次系统调用，这使得它的速度比不带读数时快了一倍。
	x := (*timeLayout)(unsafe.Pointer(&t))
	x.loc = nil // nil means UTC
	return t
}

// NowNoMono is like Now(), but it strips down the monotonic part of the
// timestamp. This is useful for getting timestamps that rounds-trip through
// various channels that strip out the monotonic part - for example yaml
// marshaling.
func NowNoMono() time.Time {
	// UTC has the side-effect of stripping the nanos.
	return time.Now().UTC()
}

// StripMono returns a copy of t with its monotonic clock reading stripped. This
// is useful for getting a time.Time that compares == with another one that
// might not have the mono part. time.Time is meant to be compared with
// Time.Equal() (which ignores the mono), not with ==, but sometimes we have a
// time.Time in a bigger struct and we want to use require.Equal() or such.
func StripMono(t time.Time) time.Time {
	// UTC() has the side-effect of stripping the mono part.
	return t.UTC()
}

// timeLayout mimics time.Time, exposing all the fields. We do an unsafe cast of
// a time.Time to this in order to set the location.
type timeLayout struct {
	wall uint64
	ext  int64
	loc  *time.Location
}

// Since returns the time elapsed since t.
// It is shorthand for Now().Sub(t), but more efficient.
func Since(t time.Time) time.Duration {
	return time.Since(t)
}

// Until returns the duration until t.
// It is shorthand for t.Sub(Now()), but more efficient.
func Until(t time.Time) time.Duration {
	return time.Until(t)
}

// UnixEpoch represents the Unix epoch, January 1, 1970 UTC.
var UnixEpoch = time.Unix(0, 0).UTC()

// FromUnixMicros returns the UTC time.Time corresponding to the given Unix
// time, usec microseconds since UnixEpoch. In Go's current time.Time
// implementation, all possible values for us can be represented as a time.Time.
func FromUnixMicros(us int64) time.Time {
	return time.Unix(us/1e6, (us%1e6)*1e3).UTC()
}

// FromUnixNanos returns the UTC time.Time corresponding to the given Unix
// time, ns nanoseconds since UnixEpoch. In Go's current time.Time
// implementation, all possible values for ns can be represented as a time.Time.
func FromUnixNanos(ns int64) time.Time {
	return time.Unix(ns/1e9, ns%1e9).UTC()
}

// ToUnixMicros returns t as the number of microseconds elapsed since UnixEpoch.
// Fractional microseconds are rounded, half up, using time.Round. Similar to
// time.Time.UnixNano, the result is undefined if the Unix time in microseconds
// cannot be represented by an int64.
func ToUnixMicros(t time.Time) int64 {
	return t.Unix()*1e6 + int64(t.Round(time.Microsecond).Nanosecond())/1e3
}

// Unix wraps time.Unix ensuring that the result is in UTC instead of Local.
//
// The process of deriving the args to construct a specific time.Time:
//
//	// say we want to construct timestamp "294277-01-01 23:59:59.999999 +0000 UTC"
//	tm := time.Date(294277, 1, 1, 23, 59, 59, 999999000, time.UTC)
//	// get the args of "timeutil.Unix"
//	sec := tm.Unix()
//	nsec := int64(tm.Nanosecond())
//	// verify
//	fmt.Println(tm == time.Unix(sec, nsec).UTC())
func Unix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec).UTC()
}

// ReplaceLibPQTimePrefix replaces unparsable lib/pq dates used for timestamps
// (0000-01-01) with timestamps that can be parsed by date libraries.
func ReplaceLibPQTimePrefix(s string) string {
	if strings.HasPrefix(s, LibPQTimePrefix) {
		return "1970-01-01" + s[len(LibPQTimePrefix):]
	}
	return s
}
