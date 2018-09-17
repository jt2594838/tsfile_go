package operator

type EmptyFilter struct {

}

func (EmptyFilter) satisfy(val interface{}) bool {
	return true
}

