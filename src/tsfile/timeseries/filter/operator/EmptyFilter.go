package operator

type EmptyFilter struct {

}

func (EmptyFilter) Satisfy(val interface{}) bool {
	return true
}

