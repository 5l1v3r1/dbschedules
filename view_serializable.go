package dbschedules

import "github.com/unixpickle/approb"

// ViewSerialize computes a serial schedule which is view
// equivalent to the given schedule.
// If no such schedule exists, it returns nil.
//
// This uses an algorithm which runs in factorial time on
// the number of transactions, so it should only be used
// for short schedules.
func ViewSerialize(s Schedule) Schedule {
	c := make(chan struct{})
	for ser := range serialSchedules(s, c) {
		if viewEquivalent(s, ser) {
			close(c)
			return ser
		}
	}
	return nil
}

func serialSchedules(s Schedule, c <-chan struct{}) <-chan Schedule {
	res := make(chan Schedule)
	go func() {
		defer close(res)
		trans := s.Transactions()
		for perm := range approb.Perms(len(trans)) {
			newS := make(Schedule, 0, len(s))
			for _, j := range perm {
				transName := trans[j]
				for _, x := range s {
					if x.Transaction == transName {
						newS = append(newS, x)
					}
				}
			}
			select {
			case res <- newS:
			case <-c:
				return
			}

		}
	}()
	return res
}

func viewEquivalent(s1, s2 Schedule) bool {
	p1, f1 := accessPatterns(s1)
	p2, f2 := accessPatterns(s2)
	if len(p1) != len(p2) || len(f1) != len(f2) {
		return false
	}
	for k1, v1 := range p1 {
		if p2[k1] != v1 {
			return false
		}
	}
	for k1, v1 := range f1 {
		if f2[k1] != v1 {
			return false
		}
	}
	return true
}

func accessPatterns(s Schedule) (prev map[*Action]*Action, final map[string]*Action) {
	prev = map[*Action]*Action{}
	final = map[string]*Action{}
	for _, x := range s {
		if x.Type == Read {
			prev[x] = final[x.Object]
		} else if x.Type == Write {
			final[x.Object] = x
		}
	}
	return
}
