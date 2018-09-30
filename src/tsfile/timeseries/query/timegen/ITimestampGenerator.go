package timegen


type ITimestampGenerator interface {

	HasNext() bool

	Next() int64

	Close()
}