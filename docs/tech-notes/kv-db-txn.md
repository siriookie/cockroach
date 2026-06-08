# `kv.DB.Txn` in CockroachDB

Author: learning note


## Overview

`kv.DB.Txn` is the common helper for running a CockroachDB KV transaction with
retry handling.

The important idea is:

```text
caller gives DB.Txn a function
DB.Txn creates a kv.Txn
DB.Txn runs the function inside that kv.Txn
txn.Get / txn.Put / txn.Del build KV requests
Txn.Send sends those requests into the KV stack
CockroachDB retries the function when KV returns a retryable transaction error
```

The API looks small:

```go
func (db *DB) Txn(ctx context.Context, retryable func(context.Context, *Txn) error) error {
	return db.TxnWithAdmissionControl(
		ctx, kvpb.AdmissionHeader_OTHER, admissionpb.NormalPri,
		SteppingDisabled, retryable,
	)
}
```

But it hides three important pieces:

- transaction creation
- retry-loop execution
- admission-control metadata


## Mental Model

Think of `DB.Txn` as this helper:

```go
err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	value, err := txn.Get(ctx, key)
	if err != nil {
		return err
	}
	_ = value
	return txn.Put(ctx, otherKey, "new-value")
})
```

The callback is called `retryable` because CockroachDB may run it more than once.

For example:

```text
attempt 1:
  txn.Get(k1)
  txn.Put(k2)
  commit hits a write-write conflict
  KV returns TransactionRetryWithProtoRefreshError

attempt 2:
  DB.Txn refreshes/restarts the transaction
  DB.Txn calls the callback again
  txn.Get(k1)
  txn.Put(k2)
  commit succeeds
```

This is why code inside a `DB.Txn` callback must be retry-safe. It should not
perform external side effects that cannot be repeated, such as sending an email,
mutating a local counter used outside the transaction, or calling an external
payment API.


## What `DB.Txn` Does

`DB.Txn` is mostly a convenience wrapper. It immediately calls
`TxnWithAdmissionControl` with fixed defaults:

```text
source       = AdmissionHeader_OTHER
priority     = NormalPri
steppingMode = SteppingDisabled
```

The next layer is `DB.TxnWithAdmissionControl`:

```text
DB.Txn
  -> DB.TxnWithAdmissionControl
     -> NewTxnWithAdmissionControl
     -> txn.SetDebugName("unnamed")
     -> txn.ConfigureStepping(...)
     -> runTxn
        -> txn.exec
```

In code, `TxnWithAdmissionControl` gets the local node ID, creates a `kv.Txn`,
sets some transaction configuration, then delegates to `runTxn`.

`runTxn` is the small wrapper that calls `txn.exec`. If the transaction fails,
it tries to roll the transaction back.


## What `AdmissionHeader_OTHER` Means

The most surprising part of `DB.Txn` is this default:

```go
kvpb.AdmissionHeader_OTHER
```

CockroachDB attaches an `AdmissionHeader` to KV batches. The KV admission layer
uses it to decide whether work should wait in admission queues.

For `DB.Txn`, the source is `OTHER`:

```text
DB.Txn
  -> NewTxnWithAdmissionControl(..., source = OTHER, priority = NormalPri)
  -> txn.admissionHeader.Source = OTHER
```

When a request reaches KV admission, `OTHER` is treated as bypassing admission
control:

```text
if source == AdmissionHeader_OTHER:
  bypassAdmission = true
```

So `DB.Txn` is not just "run a transaction". It is "run a transaction using the
legacy/default KV transaction helper, whose admission source currently bypasses
KV admission control for the system tenant".

That is why the source comment in `db.go` says this API should be audited and
possibly renamed. A future name like `TxnWithoutAdmissionControl` would make the
behavior more obvious.


## Comparison With Related Helpers

`DB.Txn`:

```text
source = OTHER
effect = bypasses KV admission control in the system-tenant path
use    = old/general helper, but new callers should think carefully
```

`DB.TxnRootKV`:

```text
source = ROOT_KV
effect = KV-originated root work that should go through admission control
use    = internal KV work that should be throttled like normal admitted work
```

`DB.TxnWithSteppingEnabled`:

```text
source   = FROM_SQL
stepping = enabled
use      = SQL-originated transaction work
```

The difference is not "which one talks to Pebble". They all eventually send KV
requests through the normal distributed KV stack. The difference is the metadata
attached to the request and how admission control treats it.


## Request Path Example

Take this callback:

```go
err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	_, err := txn.Get(ctx, key)
	return err
})
```

The rough path is:

```text
db.Txn(...)
  -> db.TxnWithAdmissionControl(ctx, OTHER, NormalPri, ...)
  -> NewTxnWithAdmissionControl(...)
     -> create roachpb.Transaction
     -> set txn.admissionHeader.Source = OTHER
  -> runTxn(...)
  -> txn.exec(...)
  -> user callback
  -> txn.Get(ctx, key)
  -> build a BatchRequest
  -> txn.Send(...)
     -> fill ba.AdmissionHeader from txn.AdmissionHeader()
     -> db.sendUsingSender(...)
  -> TxnCoordSender
  -> DistSender
  -> leaseholder replica for the target range
  -> MVCC read in storage engine
```

So even though `DB.Txn` bypasses admission control, it still uses the real KV
request path:

```text
not direct Pebble access
not local-only access
not skipping range routing
not skipping leaseholder routing
not skipping transaction coordination
```


## Why The Callback Can Be Retried

CockroachDB transactions are optimistic. They can fail because another
transaction wrote conflicting data, pushed the timestamp, or forced the
transaction to refresh.

When that happens, KV can return a retryable transaction error. `txn.exec`
contains the retry machinery that can re-run the callback with refreshed
transaction state.

This is why this pattern is correct:

```go
err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	row, err := txn.Get(ctx, key)
	if err != nil {
		return err
	}
	return txn.Put(ctx, key, update(row))
})
```

But this pattern is dangerous:

```go
err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	sendEmail()
	return txn.Put(ctx, key, "done")
})
```

If the transaction retries, `sendEmail()` may run multiple times.


## Example: KV Prober

The KV prober uses this difference directly.

It builds a read function:

```go
f := ops.Read(step.Key)
```

That function validates the key and does:

```go
txn.Get(ctx, key)
```

Then the prober chooses one of two transaction runners:

```go
if bypassAdmissionControl.Get(&p.settings.SV) {
	return txns.Txn(ctx, f)
}
return txns.TxnRootKV(ctx, f)
```

Meaning:

```text
bypassAdmissionControl = true
  -> DB.Txn
  -> source OTHER
  -> bypass KV admission

bypassAdmissionControl = false
  -> DB.TxnRootKV
  -> source ROOT_KV
  -> goes through KV admission
```

This is useful because a health probe often wants to know whether the KV path is
alive even when the system is overloaded. If the probe itself waits behind the
same overloaded admission queue, it may only prove that the queue is backed up,
not whether routing, leaseholders, replicas, and storage can still answer.


## Code Reading Map

Start here:

```text
pkg/kv/db.go
  DB.Txn
  DB.TxnWithAdmissionControl
  DB.TxnRootKV
  runTxn
```

Then read:

```text
pkg/kv/txn.go
  NewTxnWithAdmissionControl
  NewTxnRootKV
  Txn.Send
```

For admission behavior:

```text
pkg/kv/kvserver/kvadmission/kvadmission.go
  workInfoForBatch
```

For a concrete caller:

```text
pkg/kv/kvprober/kvprober.go
  ProberOps.Read
  proberTxnImpl.Txn
  proberTxnImpl.TxnRootKV
  Prober.readProbeImpl
```


## Things To Remember

`DB.Txn` is a retrying transaction helper.

The callback can run more than once.

`DB.Txn` creates a `kv.Txn` with `AdmissionHeader_OTHER`.

`AdmissionHeader_OTHER` currently causes KV admission bypass in the relevant KV
admission path.

Bypassing admission control does not mean bypassing the KV stack. The request
still goes through transaction coordination, range routing, leaseholder routing,
replica execution, and MVCC/storage.

