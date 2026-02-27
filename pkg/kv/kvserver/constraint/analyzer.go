// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package constraint

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// AnalyzedConstraints represents the result of AnalyzeConstraints(). It
// combines a span config's constraints with information about which stores
// satisfy what term of the constraints disjunction.
type AnalyzedConstraints struct {
	Constraints []roachpb.ConstraintsConjunction
	// True if the per-replica constraints don't fully cover all the desired
	// replicas in the range (sum(constraints.NumReplicas) < config.NumReplicas).
	// In such cases, we allow replicas that don't match any of the per-replica
	// constraints, but never mark them as necessary.
	UnconstrainedReplicas bool
	// For each conjunction of constraints in the above slice, track which
	// StoreIDs satisfy them. This field is unused if there are no constraints.
	SatisfiedBy [][]roachpb.StoreID
	// Maps from StoreID to the indices in the constraints slice of which
	// constraints the store satisfies. This field is unused if there are no
	// constraints.
	Satisfies map[roachpb.StoreID][]int
}

// EmptyAnalyzedConstraints represents an empty set of constraints that are
// satisfied by any given configuration of replicas.
var EmptyAnalyzedConstraints = AnalyzedConstraints{}

// StoreResolver resolves a store descriptor by a given ID.
type StoreResolver interface {
	GetStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool)
}

// AnalyzeConstraints processes the span config constraints that apply to a
// range along with the current replicas for a range, spitting back out
// information about which constraints are satisfied by which replicas and
// which replicas satisfy which constraints, aiding in allocation decisions.
// 输入：
// existing []roachpb.ReplicaDescriptor → 当前 range 已存在的副本列表
// numReplicas int32 → range 的副本总数
// constraints []roachpb.ConstraintsConjunction → 当前 range 的约束配置（span config）
// 例如要求副本分布在不同机架、不同区域、不同节点类型等
// 输出：
// AnalyzedConstraints → 一个结构，记录：
// 每条约束被哪些副本满足（SatisfiedBy）
// 每个副本满足哪些约束（Satisfies）
// 是否存在未约束副本（UnconstrainedReplicas）
// 核心作用：计算现有副本和约束的匹配情况，为 allocator 决定副本迁移或扩容提供依据
// 例子：
// 假设 Range 有 3 个副本，Zone Config 有如下约束：
// 至少一个副本在 region=us-east
// 至少一个副本在 region=us-west
// 当前副本：
// Replica	Region
// R1	us-east
// R2	us-east
// R3	us-west
// 分析结果：
// SatisfiedBy[0] = [R1, R2] → 第一个约束（us-east）被 R1 和 R2 满足
// SatisfiedBy[1] = [R3] → 第二个约束（us-west）被 R3 满足
// Satisfies[R1] = [0]
// Satisfies[R3] = [1]
// allocator 根据这个结果知道：
// 哪些副本已经满足约束
// 哪些约束需要额外副本
// 哪些副本是 unconstrained（可以灵活迁移）
func AnalyzeConstraints(
	storeResolver StoreResolver,
	existing []roachpb.ReplicaDescriptor,
	numReplicas int32,
	constraints []roachpb.ConstraintsConjunction,
) AnalyzedConstraints {
	result := AnalyzedConstraints{
		Constraints: constraints,
	}

	if len(constraints) > 0 {
		result.SatisfiedBy = make([][]roachpb.StoreID, len(constraints))
		result.Satisfies = make(map[roachpb.StoreID][]int)
	}

	var constrainedReplicas int32
	//外层 for 遍历每条约束（可能有多个约束集合，每个子约束可能要求放置在不同区域 / rack / 节点类型等）
	//constrainedReplicas += subConstraints.NumReplicas → 累计要求被约束的副本数量
	//内层 for 遍历现有副本：
	//获取副本对应的 store 信息
	//如果 store 信息不存在（!ok） → 假设副本有效
	//注释解释：这种情况极少发生，出现时不抛异常，以免触发副本疯狂迁移
	//调用 CheckStoreConjunction(store, subConstraints.Constraints) → 判断该副本是否满足约束
	//满足约束时：
	//把 storeID 加入 SatisfiedBy[i]
	//记录 storeID 满足的约束索引 i
	//核心逻辑：计算每个约束被哪些副本满足，以及每个副本满足哪些约束
	//如果有 约束副本，但 总副本数比约束要求多
	//则存在 未被约束的副本（Unconstrained）
	//allocator 后续可能会对这些副本做宽松处理
	for i, subConstraints := range constraints {
		constrainedReplicas += subConstraints.NumReplicas
		for _, repl := range existing {
			// If for some reason we don't have the store descriptor (which shouldn't
			// happen once a node is hooked into gossip), trust that it's valid. This
			// is a much more stable failure state than frantically moving everything
			// off such a node.
			store, ok := storeResolver.GetStoreDescriptor(repl.StoreID)
			if !ok || CheckStoreConjunction(store, subConstraints.Constraints) {
				result.SatisfiedBy[i] = append(result.SatisfiedBy[i], store.StoreID)
				result.Satisfies[store.StoreID] = append(result.Satisfies[store.StoreID], i)
			}
		}
	}
	if constrainedReplicas > 0 && constrainedReplicas < numReplicas {
		result.UnconstrainedReplicas = true
	}
	return result
}

// CheckConjunction checks the given attributes and locality tags against all
// the given constraints. Every constraint must be satisfied by any
// attribute/tier, i.e. they are ANDed together.
func CheckConjunction(
	storeAttrs, nodeAttrs roachpb.Attributes,
	nodeLocality roachpb.Locality,
	constraints []roachpb.Constraint,
) bool {
	for _, constraint := range constraints {
		matchesConstraint := roachpb.MatchesConstraint(storeAttrs, nodeAttrs, nodeLocality, constraint)
		if (constraint.Type == roachpb.Constraint_REQUIRED && !matchesConstraint) ||
			(constraint.Type == roachpb.Constraint_PROHIBITED && matchesConstraint) {
			return false
		}
	}
	return true
}

// CheckStoreConjunction checks a store against a single set of constraints (out of
// the possibly numerous sets that apply to a range), returning true iff the
// store matches the constraints. The constraints are AND'ed together; a store
// matches the conjunction if it matches all of them.
func CheckStoreConjunction(
	storeDesc roachpb.StoreDescriptor, constraints []roachpb.Constraint,
) bool {
	return CheckConjunction(
		storeDesc.Attrs, storeDesc.Node.Attrs, storeDesc.Node.Locality, constraints)
}
