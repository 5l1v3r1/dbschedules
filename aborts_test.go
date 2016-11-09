package dbschedules

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestRecoverable(t *testing.T) {
	schedules := map[string]bool{
		"W1(A) R2(A) C2 C1":             false,
		"R1(A) W1(A) R2(A) W2(A) C1 C2": true,
		"R1(A) W1(A) R2(A) W2(A) C2 C1": false,
	}
	runAbortsTests(schedules, t, Recoverable)
}

func TestACA(t *testing.T) {
	schedules := map[string]bool{
		"W1(A) R2(A) C2 C1":             false,
		"R1(A) W1(A) R2(A) W2(A) C1 C2": false,
		"R1(A) W1(A) R2(A) W2(A) C2 C1": false,
		"R2(A) R1(A) W1(A) W2(A) A1 C2": true,
		"W1(A) W1(C) W1(A) C1":          true,
		"W1(A) R1(C) W1(A) C1":          true,
	}
	runAbortsTests(schedules, t, ACA)
}

func TestStrict(t *testing.T) {
	schedules := map[string]bool{
		"W1(A) R2(A) C2 C1":             false,
		"R1(A) W1(A) R2(A) W2(A) C1 C2": false,
		"R1(A) W1(A) R2(A) W2(A) C2 C1": false,
		"R2(A) R1(A) W1(A) W2(A) A1 C2": false,
		"R1(A) W1(A) R2(B) C1 W2(A) C2": true,
		"W1(A) W1(C) W1(A) C1":          true,
		"W1(A) R1(C) W1(A) C1":          true,
	}
	runAbortsTests(schedules, t, Strict)
}

func TestRecoverableOrdering(t *testing.T) {
	for i := 0; i < 10000; i++ {
		s := randomSchedule(false)
		rec := Recoverable(s)
		aca := ACA(s)
		strict := Strict(s)
		if strict && !aca {
			t.Fatalf("strict does not imply ACA in: %s", s)
		}
		if aca && !rec {
			t.Fatalf("ACA does not imply recoverable in: %s", s)
		}
	}
	for i := 0; i < 10000; i++ {
		s := randomSchedule(true)
		aca := ACA(s)
		strict := Strict(s)
		if strict && !aca {
			t.Fatalf("strict does not imply ACA in: %s", s)
		}
	}
}

func runAbortsTests(m map[string]bool, t *testing.T, checker func(s Schedule) bool) {
	for s, expected := range m {
		sched, err := ParseSchedule(s)
		if err != nil {
			t.Errorf("schedule %s: error %s", s, err)
			continue
		}
		actual := checker(sched)
		if actual != expected {
			t.Errorf("expected %v got %v: %s", expected, actual, s)
		}
	}
}

func randomSchedule(doAbort bool) Schedule {
	objects := []string{"A", "B", "C", "D"}
	transactions := map[string]bool{}
	for i := 1; i < rand.Intn(4)+2; i++ {
		transactions[strconv.Itoa(i)] = true
	}

	res := Schedule{}
	for i := 0; i < rand.Intn(20)+1 && len(transactions) > 0; i++ {
		allTrans := make([]string, 0, len(transactions))
		for t := range transactions {
			allTrans = append(allTrans, t)
		}
		t := allTrans[rand.Intn(len(allTrans))]
		maxAction := 3
		if doAbort {
			maxAction++
		}
		switch rand.Intn(maxAction) {
		case 0:
			res = append(res, &Action{
				Type:        Write,
				Object:      objects[rand.Intn(len(objects))],
				Transaction: t,
			})
		case 1:
			res = append(res, &Action{
				Type:        Read,
				Object:      objects[rand.Intn(len(objects))],
				Transaction: t,
			})
		case 2:
			res = append(res, &Action{
				Type:        Commit,
				Transaction: t,
			})
			delete(transactions, t)
		case 3:
			res = append(res, &Action{
				Type:        Abort,
				Transaction: t,
			})
			delete(transactions, t)
		}
	}

	for t := range transactions {
		res = append(res, &Action{Type: Commit, Transaction: t})
	}

	return res
}
