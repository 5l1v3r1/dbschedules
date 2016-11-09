package dbschedules

// Recoverable checks if a schedule is recoverable.
// The schedule should not contain any Abort actions.
func Recoverable(s Schedule) bool {
	lastWrite := map[string]string{}
	committed := map[string]bool{"": true}
	deps := map[string]map[string]bool{}
	for _, t := range s.Transactions() {
		deps[t] = map[string]bool{}
	}
	for _, x := range s {
		switch x.Type {
		case Commit:
			committed[x.Transaction] = true
			for t := range deps[x.Transaction] {
				if !committed[t] {
					return false
				}
			}
		case Read:
			deps[x.Transaction][lastWrite[x.Object]] = true
		case Write:
			lastWrite[x.Object] = x.Transaction
		}
	}
	return true
}

// ACA checks if a schedule avoids cascading aborts.
func ACA(s Schedule) bool {
	return acaOrStrict(s, false)
}

// Strict checks if a schedule is strict.
func Strict(s Schedule) bool {
	return acaOrStrict(s, true)
}

func acaOrStrict(s Schedule, strict bool) bool {
	objectLocks := map[string]string{}
	for _, x := range s {
		switch x.Type {
		case Write:
			if strict {
				if _, ok := objectLocks[x.Object]; ok {
					return false
				}
			}
			objectLocks[x.Object] = x.Transaction
		case Read:
			if _, ok := objectLocks[x.Object]; ok {
				return false
			}
		case Commit, Abort:
			freeObjs := map[string]bool{}
			for obj, t := range objectLocks {
				if t == x.Transaction {
					freeObjs[obj] = true
				}
			}
			for x := range freeObjs {
				delete(objectLocks, x)
			}
		}
	}
	return true
}
