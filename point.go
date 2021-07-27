package main

type points []*point

func (p points) Len() int {
	return len(p)
}

func (p points) Less(i, j int) bool {
	return p[i].Value < p[j].Value
}

func (p points) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
