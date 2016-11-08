package dbschedules

import (
	"fmt"
	"strings"
)

// A Schedule is a chronologically-ordered set of actions.
type Schedule []*Action

// ParseSchedule parses a space-separated list of actions.
//
// For example, a schedule could look like
// "R1(X) W2(X) C1".
func ParseSchedule(s string) (Schedule, error) {
	var res Schedule
	for i, x := range strings.Split(s, " ") {
		act, err := ParseAction(x)
		if err != nil {
			return nil, fmt.Errorf("action %d: %s", i, err)
		}
		res = append(res, act)
	}
	return res, nil
}

// String converts the schedule to a space-separated list
// of action strings.
func (s Schedule) String() string {
	comps := make([]string, len(s))
	for i, x := range s {
		comps[i] = x.String()
	}
	return strings.Join(comps, " ")
}
