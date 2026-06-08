# EWMA in CockroachDB

Author: learning note


## Overview

EWMA is short for **Exponentially Weighted Moving Average**. In Chinese, it is
usually called 指数加权移动平均.

It is used when a system keeps receiving noisy samples and wants a stable recent
trend without storing a full history.

For example, CPU usage can jump like this:

```text
10, 90, 20, 80, 30
```

If CockroachDB publishes the raw value directly, load information will shake a
lot. EWMA turns the stream into a smoother value by giving recent samples more
weight and older samples less weight.

The core formula is:

```text
new_average = new_sample * alpha + old_average * (1 - alpha)
```

`alpha` is the decay factor:

```text
larger alpha:
  reacts faster to new samples, but is noisier

smaller alpha:
  reacts slower to new samples, but is smoother
```


## Implementation Used Here

CockroachDB imports EWMA from:

```go
github.com/VividCortex/ewma
```

The version is declared in `go.mod`:

```text
github.com/VividCortex/ewma v1.1.1
```

The package exposes a small interface:

```go
type MovingAverage interface {
	Add(float64)
	Value() float64
	Set(float64)
}
```

`Add` adds a new sample.

`Value` reads the current moving average.

`Set` directly replaces the current average.


## SimpleEWMA

`SimpleEWMA` is the cheap implementation. It has one float64 field holding the
current average.

The default decay factor is:

```text
alpha = 2 / (30 + 1) = 0.064516...
```

Its update behavior is:

```text
if current average is 0:
  current average = new sample
else:
  current average = new sample * alpha + current average * (1 - alpha)
```

This implementation treats `0` as "uninitialized". That is cheap, but it means a
real stream that frequently returns to exactly zero can have special behavior:
the next non-zero sample may reset the average more sharply than expected.

CockroachDB uses `SimpleEWMA` for RPC per-peer round-trip latency metrics:

```go
roundTripLatency: &ThreadSafeMovingAverage{ma: &ewma.SimpleEWMA{}},
```

That code is in `pkg/rpc/metrics.go`.


## VariableEWMA

`VariableEWMA` is used when the caller passes a custom age:

```go
ewma.NewMovingAverage(age)
```

Its decay factor is:

```text
alpha = 2 / (age + 1)
```

So if `age = 20`:

```text
alpha = 2 / 21 = 0.095238...
```

This means the newest sample contributes about 9.5 percent to the new average.
The previous average contributes about 90.5 percent.

`VariableEWMA` also has a warm-up phase:

```text
first 10 samples:
  accumulate samples
  Value() returns 0

11th sample:
  initialize average from the first 10 samples
  then apply EWMA formula

after warm-up:
  apply EWMA formula on every Add
```

CockroachDB uses this form in the node CPU capacity provider:

```go
monitor.mu.usageEWMA = ewma.NewMovingAverage(config.CPUUsageMovingAverageAge)
```

The default age is defined as:

```go
DefaultCPUUsageMovingAverageAge = 20
```

That means node CPU usage is smoothed across a recent window where roughly the
last 20 measurements still matter, with older measurements fading out.


## Example: Node CPU Usage

The relevant code is in `pkg/kv/kvserver/load/node_capacity_provider.go`.

The monitor reads cumulative process CPU time:

```go
userTimeMillis, sysTimeMillis, err := status.GetProcCPUTime(ctx)
```

This value is cumulative. It is not "CPU used in this one second"; it is "how
much CPU time this process has used since it started".

So the code converts it to nanoseconds:

```go
totalUsageNanos := float64(userTimeMillis*1e6 + sysTimeMillis*1e6)
```

Then it computes the delta from the previous sample:

```go
delta = totalUsageNanos - lastTotalUsageNanos
```

That delta is the CPU time consumed since the last sample. The code adds this
delta into EWMA:

```go
m.mu.usageEWMA.Add(totalUsageNanos - m.mu.lastTotalUsageNanos)
```

Then it remembers the current cumulative value:

```go
m.mu.lastTotalUsageNanos = totalUsageNanos
```

Later, `GetCPUStats` converts the average delta into a per-second rate:

```go
cpuUsageNanoPerSec = int64(m.mu.usageEWMA.Value() / m.usageRefreshInterval.Seconds())
```

If the refresh interval is one second, then:

```text
EWMA value = average CPU nanoseconds consumed per sample
per-second CPU usage = EWMA value / 1 second
```

If the process consumes 500ms of CPU time in one wall-clock second, the rate is:

```text
500,000,000 CPU-ns/sec
```

That is about half of one logical CPU.

If it consumes 2 seconds of CPU time in one wall-clock second, the rate is:

```text
2,000,000,000 CPU-ns/sec
```

That is about two logical CPUs.


## Why Use EWMA Here

CPU usage has bursts. A raw sample may be high just because one short operation
ran during that interval. Another sample may be low because the process was
between bursts.

The node capacity provider wants a load signal that is:

```text
recent:
  old samples should fade away

stable:
  one spike should not dominate the signal

cheap:
  the monitor should not store a long list of samples
```

EWMA fits this well. It stores only the current average and updates it in O(1)
time for every sample.

This smoothed CPU usage is placed into `roachpb.NodeCapacity`:

```go
return roachpb.NodeCapacity{
	NodeCPURateCapacity: cpuCapacityNanoPerSec,
	NodeCPURateUsage:    cpuUsageNanoPerSec,
}
```

That value can then be included in store descriptors and used by the rest of the
KV layer as node-level load information.


## Example: RPC Latency

CockroachDB also uses EWMA for network latency.

In `pkg/rpc/clock_offset.go`, `RemoteClockMonitor` keeps latency information per
remote node:

```go
avgNanos ewma.MovingAverage
```

When a new round-trip latency measurement arrives, it updates the moving average:

```go
info.avgNanos.Add(float64(roundTripLatency.Nanoseconds()))
```

This is useful because RPC latency is noisy:

```text
10ms, 11ms, 200ms, 12ms, 13ms
```

A single slow heartbeat should not permanently make the peer look slow, but it
also should not be ignored. EWMA absorbs the spike and then gradually decays back
toward the normal value if later samples are low.


## When EWMA Is a Good Fit

EWMA is a good fit for:

```text
CPU usage
memory pressure trends
RPC latency
queue wait time
request rate
error rate
disk IO latency
```

The common pattern is that the system receives a stream of numbers and needs a
recent trend.


## When EWMA Is Not Enough

EWMA is not a replacement for a histogram or percentile metric.

For example, these two latency streams can have a similar EWMA:

```text
stream A:
  20ms, 20ms, 20ms, 20ms

stream B:
  1ms, 1ms, 1ms, 76ms
```

The average can look similar, but the user experience is different. If tail
latency matters, use histograms or percentile tracking.

EWMA is also not ideal when the exact sample window matters. It does not answer:

```text
what was the exact average over the last 60 seconds?
```

It answers a softer question:

```text
what is the recent smoothed trend?
```


## Mental Model

Think of EWMA as a "sticky gauge".

Every new sample pulls the gauge toward itself, but only part of the way:

```text
old average = 100
new sample = 200
alpha = 0.1

new average = 200 * 0.1 + 100 * 0.9
            = 20 + 90
            = 110
```

The sample says "go to 200", but the EWMA only moves to 110. If more high samples
arrive, the average continues moving upward. If low samples arrive, it moves
downward.

That is why it is useful for CockroachDB load signals: it reacts to real trends
without overreacting to every single noisy sample.
