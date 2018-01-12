package franz

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestListOffsetResponseV1(t *testing.T) {
	item := ListOffsetResponseV1{
		Responses: []ListOffsetResponseV1Response{
			{
				Topic: "a",
				PartitionResponses: []ListOffsetResponseV1Partition{
					{
						Partition: 1,
						ErrorCode: 2,
						Timestamp: 3,
						Offset:    4,
					},
				},
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found ListOffsetResponseV1
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
