package dbschedules

// A ConflictGraph is a transaction conflict graph.
// An entry in a ConflictGraph stores, for a given node n,
// all of the nodes with edges pointing to n.
type ConflictGraph map[string]map[string]bool

// BuildConflictGraph builds a conflict graph for a
// schedule.
func BuildConflictGraph(s Schedule) ConflictGraph {
	res := ConflictGraph{}
	history := map[string]map[string]ActionType{}
	for _, a := range s {
		res[a.Transaction] = map[string]bool{}
		if a.Object != "" {
			history[a.Object] = map[string]ActionType{}
		}
	}
	for _, a := range s {
		if a.Object == "" {
			continue
		}
		hist := history[a.Object]
		if a.Type == Read {
			for trans, actType := range hist {
				if actType == Write && trans != a.Transaction {
					res[a.Transaction][trans] = true
				}
			}
			if _, ok := hist[a.Transaction]; !ok {
				hist[a.Transaction] = Read
			}
		} else if a.Type == Write {
			for trans := range hist {
				if trans != a.Transaction {
					res[a.Transaction][trans] = true
				}
			}
			hist[a.Transaction] = Write
		}
	}
	return res
}

// Copy creates a copy of the graph.
func (c ConflictGraph) Copy() ConflictGraph {
	res := ConflictGraph{}
	for key, val := range c {
		res[key] = map[string]bool{}
		for k, v := range val {
			res[key][k] = v
		}
	}
	return res
}

// Cyclic returns if there is a cycle in the graph.
func (c ConflictGraph) Cyclic() bool {
	graph := c.Copy()
	topNodes := map[string]bool{}
	for k, v := range graph {
		if len(v) == 0 {
			topNodes[k] = true
		}
	}

	// An N^2 algorithm for topological sort is sub-optimal
	// but, for our purposes, perfectly acceptable.
	for len(topNodes) > 0 {
		for n := range topNodes {
			delete(graph, n)
		}
		for _, v := range graph {
			for n := range topNodes {
				delete(v, n)
			}
		}
		topNodes = map[string]bool{}
		for k, v := range graph {
			if len(v) == 0 {
				topNodes[k] = true
			}
		}
	}

	return len(graph) > 0
}
