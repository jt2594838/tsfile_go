package operator

type NotFilter struct {
	inner Filter
}

func (f *NotFilter) satisfy(val interface{}) bool {
	return !f.inner.satisfy(val)
}


