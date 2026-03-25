package opset

import "fmt"

// Action represents the type of operation.
// Wire format values: MakeMap=0, Set=1, MakeList=2, Delete=3, MakeText=4, Increment=5, MakeTable=6, Mark=7
type Action uint8

const (
	ActionMakeMap   Action = 0
	ActionSet       Action = 1
	ActionMakeList  Action = 2
	ActionDelete    Action = 3
	ActionMakeText  Action = 4
	ActionIncrement Action = 5
	ActionMakeTable Action = 6
	ActionMark      Action = 7
)

func (a Action) String() string {
	switch a {
	case ActionMakeMap:
		return "MakeMap"
	case ActionSet:
		return "Set"
	case ActionMakeList:
		return "MakeList"
	case ActionDelete:
		return "Delete"
	case ActionMakeText:
		return "MakeText"
	case ActionIncrement:
		return "Increment"
	case ActionMakeTable:
		return "MakeTable"
	case ActionMark:
		return "Mark"
	default:
		return fmt.Sprintf("Action(%d)", a)
	}
}

// IsMake returns true if this action creates a new object.
func (a Action) IsMake() bool {
	switch a {
	case ActionMakeMap, ActionMakeList, ActionMakeText, ActionMakeTable:
		return true
	default:
		return false
	}
}
