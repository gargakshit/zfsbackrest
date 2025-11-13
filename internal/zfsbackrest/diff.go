package zfsbackrest

type Diff struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
}

func diffManagedDatasets(old, new []string) *Diff {
	o := make(map[string]struct{})
	n := make(map[string]struct{})
	for _, dataset := range old {
		o[dataset] = struct{}{}
	}
	for _, dataset := range new {
		n[dataset] = struct{}{}
	}

	var added, removed []string
	for dataset := range o {
		if _, ok := n[dataset]; !ok {
			removed = append(removed, dataset)
		}
	}
	for dataset := range n {
		if _, ok := o[dataset]; !ok {
			added = append(added, dataset)
		}
	}

	var diff *Diff

	if len(added) > 0 || len(removed) > 0 {
		diff = &Diff{
			Added:   added,
			Removed: removed,
		}
	}

	return diff
}
