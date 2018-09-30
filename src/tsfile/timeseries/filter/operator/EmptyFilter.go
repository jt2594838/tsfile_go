package operator

// EmptyFilter always returns true.
type EmptyFilter struct {

}

func (EmptyFilter) Satisfy(val interface{}) bool {
	return true
}

