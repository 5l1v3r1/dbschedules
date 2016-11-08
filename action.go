package dbschedules

import (
	"errors"
	"regexp"
	"strconv"
)

type ActionType int

const (
	Write ActionType = iota
	Read
	Commit
	Abort
)

var actionPattern = regexp.MustCompile("^(R|W|A|C)([^\\(\\)]+?)(\\((.+)\\))?$")
var actionNames = map[ActionType]string{Write: "W", Read: "R", Commit: "C", Abort: "A"}

type Action struct {
	// Type is the type of action.
	Type ActionType

	// Transaction is the identifier of the transaction which
	// performed this action.
	Transaction string

	// Object, if non-empty, is the name of the object
	// accessed (read or written) by the action.
	Object string
}

// ParseAction is the inverse of (*Action).String().
func ParseAction(s string) (*Action, error) {
	subm := actionPattern.FindStringSubmatch(s)
	if subm == nil {
		return nil, errors.New("invalid action: " + s)
	}

	var resType ActionType
	for t, n := range actionNames {
		if n == subm[1] {
			resType = t
			break
		}
	}

	if (resType == Commit || resType == Abort) &&
		subm[4] != "" {
		return nil, errors.New("action cannot take object: " + s)
	}

	return &Action{
		Type:        resType,
		Transaction: subm[2],
		Object:      subm[4],
	}, nil
}

// String returns a human-readable version of the Action.
//
// Action names are encoded as follows:
//
//     Write:  W
//     Read:   R
//     Abort:  A
//     Commit: C
//
// Actions are encoded as <name><transaction>(<object>),
// such as "W2(X)", "A1", "C3", "R2(Y)".
func (a *Action) String() string {
	name, ok := actionNames[a.Type]
	if !ok {
		panic("unknown action type: " + strconv.Itoa(int(a.Type)))
	}
	if a.Object == "" {
		return name + a.Transaction
	}
	return name + a.Transaction + "(" + a.Object + ")"
}
