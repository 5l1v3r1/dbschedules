package dbschedules

import (
	"reflect"
	"testing"
)

func TestActionString(t *testing.T) {
	actions := []*Action{
		{Type: Commit, Transaction: "32"},
		{Type: Abort, Transaction: "3"},
		{Type: Read, Transaction: "9", Object: "XY"},
		{Type: Write, Transaction: "ax", Object: "YX"},
	}
	strs := []string{"C32", "A3", "R9(XY)", "Wax(YX)"}
	for i, a := range actions {
		if a.String() != strs[i] {
			t.Errorf("action %d: expected %s got %s", i, strs[i], a.String())
		}
	}
}

func TestActionParse(t *testing.T) {
	actions := []*Action{
		{Type: Commit, Transaction: "32"},
		{Type: Abort, Transaction: "3"},
		{Type: Read, Transaction: "9", Object: "XY"},
		{Type: Write, Transaction: "ax", Object: "YX"},
		{Type: Write, Transaction: "ax"},
	}
	for i, a := range actions {
		s := a.String()
		parsed, err := ParseAction(s)
		if err != nil {
			t.Errorf("action %d: %s", i, err)
		} else if !reflect.DeepEqual(parsed, a) {
			t.Errorf("action %d: should be %v but got %v", i, a, parsed)
		}
	}

	shouldFail := []string{"A1(X)", "W1()", "A", "W", "T1(X)", "T3"}
	for _, x := range shouldFail {
		if _, err := ParseAction(x); err == nil {
			t.Errorf("parse should fail: %s", x)
		}
	}
}
