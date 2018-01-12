package franz

import (
	"bufio"
)

type MetadataRequestV0 []string

func (r MetadataRequestV0) size() int32 {
	return sizeofStringArray([]string(r))
}

func (r MetadataRequestV0) writeTo(w *bufio.Writer) {
	writeStringArray(w, []string(r))
}

type MetadataResponseV0 struct {
	Brokers []MetadataResponseV0Broker
	Topics  []MetadataResponseV0Topic
}

func (t MetadataResponseV0) size() int32 {
	n1 := sizeofArray(len(t.Brokers), func(i int) int32 { return t.Brokers[i].size() })
	n2 := sizeofArray(len(t.Topics), func(i int) int32 { return t.Topics[i].size() })
	return n1 + n2
}

func (t MetadataResponseV0) writeTo(w *bufio.Writer) {
	writeArray(w, len(t.Brokers), func(i int) { t.Brokers[i].writeTo(w) })
	writeArray(w, len(t.Topics), func(i int) { t.Topics[i].writeTo(w) })
}

func (t *MetadataResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	fnBroker := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		item := MetadataResponseV0Broker{}
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.Brokers = append(t.Brokers, item)
		return
	}
	if remain, err = readArrayWith(r, size, fnBroker); err != nil {
		return
	}

	fnTopic := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var item MetadataResponseV0Topic
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.Topics = append(t.Topics, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fnTopic); err != nil {
		return
	}

	return
}

type MetadataResponseV0Broker struct {
	NodeID int32
	Host   string
	Port   int32
}

func (b MetadataResponseV0Broker) size() int32 {
	return 4 + 4 + sizeofString(b.Host)
}

func (b MetadataResponseV0Broker) writeTo(w *bufio.Writer) {
	writeInt32(w, b.NodeID)
	writeString(w, b.Host)
	writeInt32(w, b.Port)
}

func (b *MetadataResponseV0Broker) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &b.NodeID); err != nil {
		return
	}
	if remain, err = readString(r, remain, &b.Host); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &b.Port); err != nil {
		return
	}
	return
}

type MetadataResponseV0Topic struct {
	TopicErrorCode int16
	TopicName      string
	Partitions     []MetadataResponseV0Partition
}

func (t MetadataResponseV0Topic) size() int32 {
	return 2 +
		sizeofString(t.TopicName) +
		sizeofArray(len(t.Partitions), func(i int) int32 { return t.Partitions[i].size() })
}

func (t MetadataResponseV0Topic) writeTo(w *bufio.Writer) {
	writeInt16(w, t.TopicErrorCode)
	writeString(w, t.TopicName)
	writeArray(w, len(t.Partitions), func(i int) { t.Partitions[i].writeTo(w) })
}

func (t *MetadataResponseV0Topic) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &t.TopicErrorCode); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.TopicName); err != nil {
		return
	}

	fn := func(r *bufio.Reader, withSize int) (fnRemain int, fnErr error) {
		var item MetadataResponseV0Partition
		if fnRemain, fnErr = (&item).readFrom(r, withSize); fnErr != nil {
			return
		}
		t.Partitions = append(t.Partitions, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}

type MetadataResponseV0Partition struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

func (t MetadataResponseV0Partition) size() int32 {
	return 2 + 4 + 4 + sizeofInt32Array(t.Replicas) + sizeofInt32Array(t.Isr)
}

func (t MetadataResponseV0Partition) writeTo(w *bufio.Writer) {
	writeInt16(w, t.PartitionErrorCode)
	writeInt32(w, t.PartitionID)
	writeInt32(w, t.Leader)
	writeInt32Array(w, t.Replicas)
	writeInt32Array(w, t.Isr)
}

func (t *MetadataResponseV0Partition) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &t.PartitionErrorCode); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.PartitionID); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.Leader); err != nil {
		return
	}
	if remain, err = readInt32Array(r, remain, &t.Replicas); err != nil {
		return
	}
	if remain, err = readInt32Array(r, remain, &t.Isr); err != nil {
		return
	}
	return
}
