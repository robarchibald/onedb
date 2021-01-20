package kafka

import (
	"context"

	lib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Producer is an interface for the confluent-kafka-go Producer struct
type Producer interface {
	AbortTransaction(ctx context.Context) error
	BeginTransaction() error
	Close()
	CommitTransaction(ctx context.Context) error
	Events() chan lib.Event
	Flush(timeoutMs int) int
	GetFatalError() error
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*lib.Metadata, error)
	InitTransactions(ctx context.Context) error
	Len() int
	Logs() chan lib.LogEvent
	OffsetsForTimes(times []lib.TopicPartition, timeoutMs int) (offsets []lib.TopicPartition, err error)
	Produce(msg *lib.Message, deliveryChan chan lib.Event) error
	ProduceChannel() chan *lib.Message
	Purge(flags int) error
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	SendOffsetsToTransaction(ctx context.Context, offsets []lib.TopicPartition, consumerMetadata *lib.ConsumerGroupMetadata) error
	SetOAuthBearerToken(oauthBearerToken lib.OAuthBearerToken) error
	SetOAuthBearerTokenFailure(errstr string) error
	String() string
	TestFatalError(code lib.ErrorCode, str string) lib.ErrorCode
}
