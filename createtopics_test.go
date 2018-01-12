package franz

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestCreateTopicsResponseV2(t *testing.T) {
	item := createTopicsResponseV2{
		ThrottleTimeMS: 1,
		TopicErrors: []createTopicsResponseV2TopicError{
			{
				Topic:        "topic",
				ErrorCode:    2,
				ErrorMessage: "topic error",
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found createTopicsResponseV2
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

func createTopic(t *testing.T, topic string, partitions int) {
	conn, err := Dial("tcp", "localhost:9092")
	if err != nil {
		t.Error("bad conn")
		return
	}
	defer conn.Close()

	_, err = conn.createTopics(createTopicsRequestV2{
		Topics: []createTopicsRequestV2Topic{
			{
				Topic:             topic,
				NumPartitions:     int32(partitions),
				ReplicationFactor: 1,
			},
		},
		Timeout: int32(30 * time.Second / time.Millisecond),
	})
	switch err {
	case nil:
		// ok
	case TopicAlreadyExists:
		// ok
	default:
		t.Error("bad createTopics", err)
		t.FailNow()
	}
}
