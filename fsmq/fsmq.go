package fsmq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/oxia-db/oxia/oxia"
)

type Client struct {
	oxiaCli oxia.AsyncClient
}

func NewClient(serviceUrl string, namespace string) (*Client, error) {
	oxiaCli, err := oxia.NewAsyncClient(serviceUrl, oxia.WithNamespace(namespace))
	if err != nil {
		return nil, err
	}
	return &Client{
		oxiaCli: oxiaCli,
	}, nil
}

func (c *Client) Close() {
	c.oxiaCli.Close()
}

type MessageId string

type SendResult struct {
	MessageId *MessageId
	Err       error
}

func (c *Client) Send(topic string, value []byte) <-chan SendResult {
	size := len(value)
	ch := make(chan SendResult)
	putCh := c.oxiaCli.Put(topic, value, oxia.PartitionKey(topic), oxia.SequenceKeysDeltas(1, uint64(size)))
	go func() {
		putResult := <-putCh
		result := &SendResult{
			Err: putResult.Err,
		}
		if putResult.Err == nil {
			msgId := MessageId(putResult.Key)
			result.MessageId = &msgId
		}
		ch <- *result
	}()

	return ch
}

type Message struct {
	Value            []byte
	MessageId        MessageId
	CreatedTimestamp uint64
}

type ConsumeResult struct {
	Message *Message
	Err     error
}

func (c *Client) Consume(ctx context.Context, topic string, startOffset uint64, endOffset uint64) <-chan ConsumeResult {
	ch := make(chan ConsumeResult)

	go func() {
		var lastReadPos *IndexKey
		for {
			start := startOffset
			if lastReadPos != nil {
				start = lastReadPos.Offset
			}
			key := &IndexKey{
				Topic:  topic,
				Offset: start,
				Size:   uint64(math.MaxUint64),
			}
			endKey := &IndexKey{
				Topic:  topic,
				Offset: endOffset,
				Size:   uint64(math.MaxUint64),
			}

			for result := range c.oxiaCli.RangeScan(ctx, key.ToString(), endKey.ToString(), oxia.PartitionKey(topic)) {
				if result.Err != nil {
					ch <- ConsumeResult{
						Err: result.Err,
					}
					continue
				}
				indexKey, err := ParseIndexKey(result.Key)
				if err != nil {
					ch <- ConsumeResult{
						Err: err,
					}
					continue
				}
				if lastReadPos != nil && indexKey.Offset <= lastReadPos.Offset {
					continue
				}
				lastReadPos = &indexKey
				ch <- ConsumeResult{
					Message: &Message{
						Value:            result.Value,
						MessageId:        MessageId(result.Key),
						CreatedTimestamp: result.Version.CreatedTimestamp,
					},
				}
			}
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:

			}
		}

	}()
	return ch
}

const (
	numberWidth = 20
	sep         = '-'
)

type IndexKey struct {
	Topic  string
	Offset uint64
	Size   uint64
}

func (k IndexKey) ToString() string {
	return fmt.Sprintf(
		"%s-%020d-%020d",
		k.Topic,
		k.Offset,
		k.Size,
	)
}

func ParseIndexKey(s string) (IndexKey, error) {
	// Minimum length:
	// topic(>=1) + "-" + 20 + "-" + 20
	minLen := 1 + 1 + numberWidth + 1 + numberWidth
	if len(s) < minLen {
		return IndexKey{}, errors.New("invalid index key: too short")
	}

	// Parse size (last 20 chars)
	sizeStart := len(s) - numberWidth
	sizeStr := s[sizeStart:]

	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		return IndexKey{}, fmt.Errorf("invalid size: %w", err)
	}

	// Expect '-'
	if s[sizeStart-1] != sep {
		return IndexKey{}, errors.New("invalid index key: missing size separator")
	}

	// Parse offset (previous 20 chars)
	offsetStart := sizeStart - 1 - numberWidth
	offsetStr := s[offsetStart : sizeStart-1]

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		return IndexKey{}, fmt.Errorf("invalid offset: %w", err)
	}

	// Expect '-'
	if s[offsetStart-1] != sep {
		return IndexKey{}, errors.New("invalid index key: missing offset separator")
	}

	// Remaining prefix is topic
	topic := s[:offsetStart-1]
	if topic == "" {
		return IndexKey{}, errors.New("invalid index key: empty topic")
	}

	return IndexKey{
		Topic:  topic,
		Offset: offset,
		Size:   size,
	}, nil
}
