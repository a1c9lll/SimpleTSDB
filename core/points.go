package core

type Points []*Point

func (p Points) Len() int {
	return len(p)
}

func (p Points) Less(i, j int) bool {
	return p[i].Value < p[j].Value
}

func (p Points) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
