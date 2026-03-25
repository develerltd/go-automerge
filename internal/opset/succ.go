package opset

import "github.com/develerltd/go-automerge/internal/types"

// DerivePredFromSucc populates Pred from Succ on a slice of ops.
// Existing Pred values are cleared first.
func DerivePredFromSucc(ops []Op) {
	opIndex := make(map[types.OpId]int, len(ops))
	for i := range ops {
		opIndex[ops[i].ID] = i
		ops[i].Pred = nil
	}
	for i := range ops {
		for _, succId := range ops[i].Succ {
			if succIdx, ok := opIndex[succId]; ok {
				ops[succIdx].Pred = append(ops[succIdx].Pred, ops[i].ID)
			}
		}
	}
}

// DeriveSuccFromPred populates Succ from Pred on a slice of ops.
// Existing Succ values are cleared first.
func DeriveSuccFromPred(ops []Op) {
	opIndex := make(map[types.OpId]int, len(ops))
	for i := range ops {
		opIndex[ops[i].ID] = i
		ops[i].Succ = nil
	}
	for i := range ops {
		for _, predId := range ops[i].Pred {
			if predIdx, ok := opIndex[predId]; ok {
				ops[predIdx].Succ = append(ops[predIdx].Succ, ops[i].ID)
			}
		}
	}
}
