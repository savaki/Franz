package franz

import "bufio"

type ListOffsetRequestV1 struct {
	ReplicaID int32
	Topics    []ListOffsetRequestV1Topic
}

func (r ListOffsetRequestV1) size() int32 {
	return 4 + sizeofArray(len(r.Topics), func(i int) int32 { return r.Topics[i].size() })
}

func (r ListOffsetRequestV1) writeTo(w *bufio.Writer) {
	writeInt32(w, r.ReplicaID)
	writeArray(w, len(r.Topics), func(i int) { r.Topics[i].writeTo(w) })
}

type ListOffsetRequestV1Topic struct {
	TopicName  string
	Partitions []ListOffsetRequestV1Partition
}

func (t ListOffsetRequestV1Topic) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t ListOffsetRequestV1Topic) writeTo(w *bufio.Writer) {
	writeString(w, t.TopicName)
	writeArray(w, len(t.Partitions), func(i int) { t.Partitions[i].writeTo(w) })
}

type ListOffsetRequestV1Partition struct {
	Partition int32
	Time      int64
}

func (p ListOffsetRequestV1Partition) size() int32 {
	return 4 + 8
}

func (p ListOffsetRequestV1Partition) writeTo(w *bufio.Writer) {
	writeInt32(w, p.Partition)
	writeInt64(w, p.Time)
}

type ListOffsetResponseV1 []ListOffsetResponseV1Topic

func (t ListOffsetResponseV1) size() int32 {
	return sizeofArray(len(t), func(i int) int32 { return t[i].size() })
}

func (t ListOffsetResponseV1) writeTo(w *bufio.Writer) {
	writeArray(w, len(t), func(i int) { t[i].writeTo(w) })
}

func (t *ListOffsetResponseV1) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	return 0, nil
}

type ListOffsetResponseV1Topic struct {
	TopicName  string
	Partitions []ListOffsetResponseV1Partition
}

func (t ListOffsetResponseV1Topic) size() int32 {
	return sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t ListOffsetResponseV1Topic) writeTo(w *bufio.Writer) {
	writeString(w, t.TopicName)
	writeArray(w, len(t.Partitions), func(i int) { t.Partitions[i].writeTo(w) })
}

type ListOffsetResponseV1Partition struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

func (p ListOffsetResponseV1Partition) size() int32 {
	return 4 + 2 + 8 + 8
}

func (p ListOffsetResponseV1Partition) writeTo(w *bufio.Writer) {
	writeInt32(w, p.Partition)
	writeInt16(w, p.ErrorCode)
	writeInt64(w, p.Timestamp)
	writeInt64(w, p.Offset)
}

func (p *ListOffsetResponseV1Partition) readFrom(r *bufio.Reader, sz int) (remain int, err error) {
	if remain, err = readInt32(r, sz, &p.Partition); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &p.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Timestamp); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &p.Offset); err != nil {
		return
	}
	return
}
