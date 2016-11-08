package dbschedules

import (
	"reflect"
	"testing"
)

func TestBuildConflictGraph(t *testing.T) {
	sched, err := ParseSchedule("R1(A) W2(A) W4(B) R3(A) R3(B) W1(B)")
	if err != nil {
		t.Fatal(err)
	}
	actual := BuildConflictGraph(sched)
	expected := ConflictGraph{
		"1": map[string]bool{"4": true, "3": true},
		"2": map[string]bool{"1": true},
		"3": map[string]bool{"4": true, "2": true},
		"4": map[string]bool{},
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected %v but got %v", expected, actual)
	}
}

func TestConflictGraphCyclic(t *testing.T) {
	scheds := map[string]bool{
		"R1(A) W2(A) W4(B) R3(A) R3(B) W1(B)":             true,
		"R1(A) W2(A) W4(B) R3(A) R3(B)":                   false,
		"R1(X) R2(Y) R2(Y) W2(X) W3(Y) R1(X)":             true,
		"R1(X) R2(Y) W3(Z) W2(Y) W2(X) R1(Z) W3(Y) W2(X)": true,
		"R1(X) W1(Y) R2(X) W2(Z) R2(Y) W3(X) R3(Z)":       false,
		"R1(X) W1(X) R1(X)":                               true,
	}
	for sched, expected := range scheds {
		s, err := ParseSchedule(sched)
		if err != nil {
			t.Errorf("failed to parse: %s", sched)
			continue
		}
		if BuildConflictGraph(s).Cyclic() != expected {
			t.Errorf("expected %v but got %v for: %s", expected, !expected, sched)
		}
	}
}
