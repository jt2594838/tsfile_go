package operator

type AndFilter struct {
	left Filter
	right Filter
}

func (f *AndFilter) satisfy(val interface{}) bool {
	return f.left.satisfy(val) && f.right.satisfy(val)
}
