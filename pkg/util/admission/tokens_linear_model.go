// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

// tokensLinearModel represents a model y = multiplier.x + constant.
type tokensLinearModel struct {
	multiplier float64
	// constant >= 0
	constant int64
}

func (m tokensLinearModel) applyLinearModel(b int64) int64 {
	return int64(float64(b)*m.multiplier) + m.constant
}

// tokensLinearModelFitter fits y = multiplier.x + constant, based on the
// current interval and then exponentially smooths the multiplier and
// constant.
//
// This fitter is probably poor and could be improved by taking history into
// account in a cleverer way, such as looking at many past samples and doing
// linear regression, under the assumption that the workload is stable.
// However, the simple approach here should be an improvement on the additive
// approach we previously used.
//
// TODO(sumeer): improve the model based on realistic combinations of
// workloads (e.g. foreground writes + index backfills).
type tokensLinearModelFitter struct {
	// [multiplierMin, multiplierMax] constrains the multiplier.
	multiplierMin float64
	multiplierMax float64

	intLinearModel                tokensLinearModel
	smoothedLinearModel           tokensLinearModel
	smoothedPerWorkAccountedBytes int64

	// Should be set to true for the L0 ingested bytes model: if all bytes are
	// ingested below L0, the actual bytes will be zero and the accounted bytes
	// non-zero. We need to update the model in this case.
	updateWithZeroActualNonZeroAccountedForL0IngestedModel bool
}

func makeTokensLinearModelFitter(
	multMin float64, multMax float64, updateWithZeroActualNonZeroAccountedForL0IngestedModel bool,
) tokensLinearModelFitter {
	return tokensLinearModelFitter{
		multiplierMin: multMin,
		multiplierMax: multMax,
		smoothedLinearModel: tokensLinearModel{
			multiplier: (multMin + multMax) / 2,
			constant:   1,
		},
		smoothedPerWorkAccountedBytes:                          1,
		updateWithZeroActualNonZeroAccountedForL0IngestedModel: updateWithZeroActualNonZeroAccountedForL0IngestedModel,
	}
}

// updateModelUsingIntervalStats updates the model, based on various stats
// over the last interval: the number of work items admitted (workCount), the
// bytes claimed by these work items (accountedBytes), and the actual bytes
// observed in the LSM for that interval (actualBytes).
//
// As mentioned store_token_estimation.go, the current fitting algorithm is
// probably poor, though an improvement on what we had previously. The approach
// taken is:
//
//   - Fit the best model we can for the interval,
//     multiplier*accountedBytes + workCount*constant = actualBytes, while
//     minimizing the constant. We prefer the model to use the multiplier for
//     most of what it needs to account for actualBytes.
//     This exact model ignores inaccuracies due to integer arithmetic -- we
//     don't care about rounding errors since an error of 2 bytes per request is
//     inconsequential.
//
//   - The multiplier has to conform to the [min,max] configured for this model,
//     and constant has to conform to a value >= 1. The constant is constrained
//     to be >=1 on the intuition that we want a request to consume at least 1
//     token -- it isn't clear that this intuition is meaningful in any way.
//
//   - Exponentially smooth this exact model's multiplier and constant based on
//     history.
//
// Linear Model 的更新
// **职责**：基于 interval 统计，更新 linear model（y = multiplier * x + constant）
//
// **输入**：
// - `accountedBytes`：本周期内，请求声称的字节数
// - `actualBytes`：本周期内，实际观察到的 LSM 字节数
// - `workCount`：本周期内的请求数量
// 输出：
// - 更新 smoothedLinearModel
// **拟合算法的数学推导**：
//
// ```
// 目标：拟合 y = a*x + b*n，其中
// ├─ y = actualBytes（实际观察到的 LSM 字节数）
// ├─ x = accountedBytes（请求声称的字节数）
// ├─ n = workCount（请求数量）
// ├─ a = multiplier（要求解）
// └─ b = constant（要求解）
//
// 约束：
// ├─ a ∈ [multiplierMin, multiplierMax]
// ├─ b >= 1
// └─ 优先使用 multiplier，minimizing constant
//
// 步骤 1：先设 b = 1（最小值）
//
// 步骤 2：求解 a
// ├─ a*x + 1*n = y
// ├─ a*x = y - n
// └─ a = (y - n) / x
//
// 步骤 3：如果 a 超出范围，clip 到边界
// ├─ if a > max: a = max
// └─ if a < min: a = min
//
// 步骤 4：检查是否能完全解释 y
// ├─ modelBytes = a*x + b*n
// └─ if modelBytes < y:
//
//	├─ 模型低估，需要增加 b
//	└─ b += (y - modelBytes) / n
//
// 示例：
// 假设
// ├─ accountedBytes = 1000 MB
// ├─ actualBytes = 1800 MB
// ├─ workCount = 100
// ├─ [min, max] = [0.5, 3.0]
//
// 步骤 1: b = 1
//
// 步骤 2: a = (1800 - 100*1) / 1000 = 1.7
//
// 步骤 3: 1.7 ∈ [0.5, 3.0] ✓
//
// 步骤 4: modelBytes = 1.7*1000 + 1*100 = 1800
//
//	1800 == 1800 ✓ 无需调整
//
// 最终模型：y = 1.7*x + 1
// ```
//
// **平滑的效果**：
//
// ```
// 假设历史模型：
// ├─ smoothedMultiplier = 1.5
// └─ smoothedConstant = 100
//
// 本周期拟合的模型：
// ├─ intMultiplier = 1.7
// └─ intConstant = 1
//
// 平滑后（α = 0.5）：
// ├─ smoothedMultiplier = 0.5 * 1.7 + 0.5 * 1.5 = 1.6
// └─ smoothedConstant = 0.5 * 1 + 0.5 * 100 = 50.5 ≈ 50
//
// 效果：
// ├─ 模型逐步调整，而非剧烈变化
// ├─ 避免单次异常数据的过度影响
// └─ 保持模型稳定性
// ```
func (f *tokensLinearModelFitter) updateModelUsingIntervalStats(
	accountedBytes int64, actualBytes int64, workCount int64,
) {
	// ===== 步骤 0：边界检查 =====
	if workCount <= 1 || (actualBytes <= 0 &&
		(!f.updateWithZeroActualNonZeroAccountedForL0IngestedModel || accountedBytes <= 0)) {
		// 数据不足，无法拟合
		// 但为避免 constant 过大持续惩罚，将其减半
		// Don't want to update the model if workCount is very low or actual bytes
		// is zero (except for the exceptions in the if-condition above).
		//
		// Not updating the model at all does have the risk that a large constant
		// will keep penalizing in the future. For example, if there are only
		// ingests, and the regular writes model had a large constant, it will
		// keep penalizing ingests. So we scale down the constant as if the new
		// model had a 0 value for the constant and the exponential smoothing
		// alpha was 0.5, i.e., halve the constant.
		f.intLinearModel = tokensLinearModel{}
		f.smoothedLinearModel.constant = max(1, f.smoothedLinearModel.constant/2)
		return
	}
	// ===== 步骤 1：防御性处理 =====
	if actualBytes < 0 {
		actualBytes = 0
	}
	const alpha = 0.5
	if accountedBytes <= 0 {
		if actualBytes > 0 {
			// 异常情况：有实际字节但无声称字节
			// 假设未来会有正常的 accountedBytes
			// Anomaly. Assume that we will see smoothedPerWorkAccountedBytes in the
			// future. This prevents us from blowing up the constant in the model due
			// to this anomaly.
			accountedBytes = workCount * max(1, f.smoothedPerWorkAccountedBytes)
		} else {
			// actualBytes is also 0.
			accountedBytes = 1
		}
	} else {
		perWorkAccountedBytes := accountedBytes / workCount
		f.smoothedPerWorkAccountedBytes = int64(
			alpha*float64(perWorkAccountedBytes) + (1-alpha)*float64(f.smoothedPerWorkAccountedBytes))
	}
	// INVARIANT: workCount > 0, accountedBytes > 0, actualBytes >= 0.

	// Start with the lower bound of 1 on constant, since we want most of bytes
	// to be fitted using the multiplier. So workCount tokens go into that.
	// ===== 步骤 2：拟合 constant（从最小值 1 开始）=====
	constant := int64(1)
	// Then compute the multiplier.
	// ===== 步骤 3：拟合 multiplier =====
	// 目标：minimizer.x + constant.count = actual
	// 求解：multiplier = (actual - constant.count) / x
	multiplier := float64(max(0, actualBytes-workCount*constant)) / float64(accountedBytes)
	// The multiplier may be too high or too low, so make it conform to
	// [min,max].
	// ===== 步骤 4：将 multiplier 限制在 [min, max] 范围内 =====
	if multiplier > f.multiplierMax {
		multiplier = f.multiplierMax
	} else if multiplier < f.multiplierMin {
		multiplier = f.multiplierMin
	}
	// This is the model with the multiplier as small or large as possible,
	// while minimizing constant (which is 1).
	// ===== 步骤 5：检查模型是否能完全解释 actualBytes =====
	modelBytes := int64(multiplier*float64(accountedBytes)) + (constant * workCount)
	// If the model is not accounting for all of actualBytes, we are forced to
	// increase the constant to cover the difference.
	if modelBytes < actualBytes {
		// 模型低估了，需要增加 constant 来弥补
		constantAdjust := (actualBytes - modelBytes) / workCount
		// Avoid overflow in case of bad stats.
		if constantAdjust+constant > 0 {
			constant += constantAdjust
		}
	}
	// The best model we can come up for the interval.
	// ===== 步骤 6：记录本周期的精确模型 =====
	f.intLinearModel = tokensLinearModel{
		multiplier: multiplier,
		constant:   constant,
	}
	// Smooth the multiplier and constant factors.
	// ===== 步骤 7：指数平滑 =====
	f.smoothedLinearModel.multiplier = alpha*multiplier + (1-alpha)*f.smoothedLinearModel.multiplier
	f.smoothedLinearModel.constant = int64(
		alpha*float64(constant) + (1-alpha)*float64(f.smoothedLinearModel.constant))
}
