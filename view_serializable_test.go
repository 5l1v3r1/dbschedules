package dbschedules

import "testing"

func TestViewSerialize(t *testing.T) {
	scheds := map[string]bool{
		"R1(A) W2(A) W4(B) R3(A) R3(B) W1(B)":             false,
		"R1(A) W2(A) W4(B) R3(A) R3(B)":                   true,
		"R1(X) R2(Y) R2(Y) W2(X) W3(Y) R1(X)":             false,
		"R1(X) R2(Y) W3(Z) W2(Y) W2(X) R1(Z) W3(Y) W2(X)": false,
		"R1(X) W1(Y) R2(X) W2(Z) R2(Y) W3(X) R3(Z)":       true,
		"R1(X) W1(X) R1(X)":                               true,
		"R1(X) W2(X) R1(X)":                               false,
		"R1(X) W2(X) W1(X)":                               false,
		"R1(X) W2(X) W1(X) W3(X)":                         true,
	}
	for sched, expected := range scheds {
		s, err := ParseSchedule(sched)
		if err != nil {
			t.Errorf("failed to parse: %s", sched)
			continue
		}
		if x := ViewSerialize(s); (x != nil) != expected {
			t.Errorf("expected %v but got %v for: %s (serialized to %v)", expected,
				!expected, sched, x)
		}
	}
}

func TestConflictSerializableContainment(t *testing.T) {
	for i := 0; i < 1000; i++ {
		s := randomSchedule(false)
		conSer := !BuildConflictGraph(s).Cyclic()
		viewSer := ViewSerialize(s) != nil
		if conSer && !viewSer {
			t.Fatalf("inconsistent containment: %s", s)
		}
	}
}
