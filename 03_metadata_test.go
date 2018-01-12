package franz

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestMetadataResponseV0(t *testing.T) {
	item := MetadataResponseV0{
		Brokers: []MetadataResponseV0Broker{
			{
				NodeID: 1,
				Host:   "a",
				Port:   2,
			},
		},
		Topics: []MetadataResponseV0Topic{
			{
				TopicErrorCode: 3,
				TopicName:      "b",
				Partitions: []MetadataResponseV0Partition{
					{
						PartitionErrorCode: 4,
						PartitionID:        5,
						Leader:             6,
						Replicas:           []int32{7, 8},
						Isr:                []int32{9, 10},
					},
				},
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found MetadataResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}
