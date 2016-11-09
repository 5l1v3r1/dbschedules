package dbschedules

import "testing"

func TestRecoverable(t *testing.T) {
	schedules := map[string]bool{
		"R1(A) W1(A) R2(A) W2(A) C1 C2": true,
		"R1(A) W1(A) R2(A) W2(A) C2 C1": false,
	}
	for s, expected := range schedules {
		sched, err := ParseSchedule(s)
		if err != nil {
			t.Errorf("schedule %s: error %s", s, err)
			continue
		}
		actual := Recoverable(sched)
		if actual != expected {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}
}

func TestACA(t *testing.T) {
	schedules := map[string]bool{
		"R1(A) W1(A) R2(A) W2(A) C1 C2": false,
		"R1(A) W1(A) R2(A) W2(A) C2 C1": false,
		"R2(A) R1(A) W1(A) W2(A) A1 C2": true,
	}
	for s, expected := range schedules {
		sched, err := ParseSchedule(s)
		if err != nil {
			t.Errorf("schedule %s: error %s", s, err)
			continue
		}
		actual := ACA(sched)
		if actual != expected {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}
}
