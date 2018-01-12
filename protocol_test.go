package franz

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestProtocol(t *testing.T) {
	t.Parallel()

	tests := []interface{}{
		int8(42),
		int16(42),
		int32(42),
		int64(42),
		"",
		"Hello World!",
		[]byte(nil),
		[]byte("Hello World!"),

		requestHeader{
			Size:          26,
			ApiKey:        int16(offsetCommitRequest),
			ApiVersion:    int16(v2),
			CorrelationID: 42,
			ClientID:      "Hello World!",
		},

		message{
			MagicByte: 1,
			Timestamp: 42,
			Key:       nil,
			Value:     []byte("Hello World!"),
		},

		MetadataRequestV0{"A", "B", "C"},

		ListOffsetRequestV1{
			ReplicaID: 1,
			Topics: []ListOffsetRequestV1Topic{
				{TopicName: "A", Partitions: []ListOffsetRequestV1Partition{
					{Partition: 0, Time: -1},
					{Partition: 1, Time: -1},
					{Partition: 2, Time: -1},
				}},
				{TopicName: "B", Partitions: []ListOffsetRequestV1Partition{
					{Partition: 0, Time: -2},
				}},
				{TopicName: "C", Partitions: []ListOffsetRequestV1Partition{
					{Partition: 0, Time: 42},
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%T", test), func(t *testing.T) {
			b := &bytes.Buffer{}
			r := bufio.NewReader(b)
			w := bufio.NewWriter(b)

			write(w, test)

			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}

			if size := int(sizeof(test)); size != b.Len() {
				t.Error("invalid size:", size, "!=", b.Len())
			}

			v := reflect.New(reflect.TypeOf(test))
			n := b.Len()

			n, err := read(r, n, v.Interface())
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Errorf("%d unread bytes", n)
			}

			if !reflect.DeepEqual(test, v.Elem().Interface()) {
				t.Error("values don't match:")
				t.Logf("expected: %#v", test)
				t.Logf("found:    %#v", v.Elem().Interface())
			}
		})
	}
}
