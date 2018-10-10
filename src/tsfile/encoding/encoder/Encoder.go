package encoder

type Encoder interface {
	Init(data []byte)
	Encode(value interface{})
	Flush() bool
}
