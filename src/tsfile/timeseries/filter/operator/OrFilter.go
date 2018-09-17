package operator

type OrFilter struct {
	left Filter
	right Filter
}

func (f *OrFilter) satisfy(val interface{}) bool {
	return f.left.satisfy(val) || f.right.satisfy(val)
}
