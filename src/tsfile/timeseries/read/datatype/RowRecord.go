package datatype

type RowRecord struct {
	timestamp int64
	paths     []string
	values    []interface{}
}

func (r *RowRecord) SetTimestamp(timestamp int64) {
	r.timestamp = timestamp
}

func NewRowRecord() *RowRecord {
	return &RowRecord{0, nil, nil}
}

func NewRowRecordWithPaths(paths []string) *RowRecord {
	return &RowRecord{0, paths, make([]interface{}, len(paths))}
}

func (r *RowRecord) Values() []interface{} {
	return r.values
}

func (r *RowRecord) Paths() []string {
	return r.paths
}

func (r *RowRecord) Timestamp() int64 {
	return r.timestamp
}
