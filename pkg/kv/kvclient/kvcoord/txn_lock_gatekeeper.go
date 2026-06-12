// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/errors"
)

// lockedSender 类似于 client.Sender，但要求调用方必须持有 TxnCoordSender
// 的锁才能发送请求。
type lockedSender interface {
	// SendLocked 发送批量请求并接收批量响应。
	// 调用此方法时要求持有 TxnCoordSender 的锁，但该锁不会在整个调用期间
	// 一直被持有。实际上，锁会在向下层 Sender 发送请求之前释放，并在响应
	// 返回后重新获取。
	// 警告：因为调用此方法期间锁会被释放并在返回前重新获取，调用方无法依赖
	// 跨调用边界的单一互斥区域。
	SendLocked(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error)
}

// txnLockGatekeeper 是一个 lockedSender，它位于 TxnCoordSender 的
// interceptor 栈的最底部，负责在发送请求时释放 TxnCoordSender 的互斥锁，
// 并在接收到响应时重新加锁。
// 它使得整个 txnInterceptor 栈可以在持锁状态下运行，而无需各自关心
// "在正确的时机解锁"这个问题。
type txnLockGatekeeper struct {
	wrapped kv.Sender
	mu      sync.Locker // 共享的锁，与 TxnCoordSender 共用

	// 如果设置为 true，则允许并发请求。如果未设置（false），并发请求会
	// 触发断言错误。
	// 只有叶子事务才应该允许并发请求 —— 叶子事务不会重启事务，也不会
	// 通过 refresh 来推进读时间戳。
	allowConcurrentRequests bool
	// requestInFlight 在请求正在被底层 wrapped sender 处理期间设置为 true。
	// 用于检测和防止事务的并发使用。
	requestInFlight bool
}

// SendLocked 实现了 lockedSender 接口。
// 它是整个 interceptor 栈的最底层，负责在真正发送网络请求前释放事务锁，
// 并在收到响应后重新加锁，从而使整个上层 interceptor 栈都能在持锁状态下
// 运行，而不需要各自处理"何时解锁"的问题。
func (gs *txnLockGatekeeper) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// 如果配置不允许并发请求（只有叶子事务才允许并发请求），则在此检测：
	//   - 并发请求通常不可行，因为客户端发送请求和 TxnCoordSender
	//     重启事务之间存在竞态条件；
	//   - 并发请求与 span refresher 也不兼容——在仍有正在飞行的
	//     请求且其 span 尚未被纳入追踪的情况下执行 refresh 是无效的。
	//
	// 特殊例外：异步心跳请求任何时候都可以发送，不受并发限制。
	if !gs.allowConcurrentRequests && !ba.IsSingleHeartbeatTxnRequest() {
		if gs.requestInFlight {
			return nil, kvpb.NewError(
				errors.AssertionFailedf("concurrent txn use detected. ba: %s", ba))
		}
		// 标记当前有一个请求正在底层 Sender 中处理，用于防止并发请求。
		gs.requestInFlight = true
		defer func() {
			gs.requestInFlight = false
		}()
	}

	// 注意这里的锁操作比较特别：在调用底层 Sender（进行网络 I/O）期间
	// 释放锁，等响应返回后再重新加锁。
	// 这样可以避免网络 I/O 阻塞期间一直持有事务锁，减少锁竞争。
	gs.mu.Unlock()
	defer gs.mu.Lock()
	return gs.wrapped.Send(ctx, ba)
}
