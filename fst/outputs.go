package fst

// PositiveIntOutputs defines the output algebra for uint64 values,
// following Lucene's PositiveIntOutputs. The outputs form a left-divisible
// monoid under addition with Common = min and Subtract = difference.

func outputCommon(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func outputSubtract(a, b uint64) uint64 {
	return a - b
}

func outputAdd(a, b uint64) uint64 {
	return a + b
}

const noOutput uint64 = 0
