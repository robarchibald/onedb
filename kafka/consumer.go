package kafka

import (
	"time"

	lib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Consumer is an interface for the confluent-kafka-go Consumer struct
type Consumer interface {
	Assign(partitions []lib.TopicPartition) (err error)
	Assignment() (partitions []lib.TopicPartition, err error)
	Close() (err error)
	Commit() ([]lib.TopicPartition, error)
	CommitMessage(m *lib.Message) ([]lib.TopicPartition, error)
	CommitOffsets(offsets []lib.TopicPartition) ([]lib.TopicPartition, error)
	Committed(partitions []lib.TopicPartition, timeoutMs int) (offsets []lib.TopicPartition, err error)
	Events() chan lib.Event
	GetConsumerGroupMetadata() (*lib.ConsumerGroupMetadata, error)
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*lib.Metadata, error)
	GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error)
	Logs() chan lib.LogEvent
	OffsetsForTimes(times []lib.TopicPartition, timeoutMs int) (offsets []lib.TopicPartition, err error)
	Pause(partitions []lib.TopicPartition) (err error)
	Poll(timeoutMs int) (event lib.Event)
	Position(partitions []lib.TopicPartition) (offsets []lib.TopicPartition, err error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	ReadMessage(timeout time.Duration) (*lib.Message, error)
	Resume(partitions []lib.TopicPartition) (err error)
	Seek(partition lib.TopicPartition, timeoutMs int) error
	SetOAuthBearerToken(oauthBearerToken lib.OAuthBearerToken) error
	SetOAuthBearerTokenFailure(errstr string) error
	StoreOffsets(offsets []lib.TopicPartition) (storedOffsets []lib.TopicPartition, err error)
	String() string
	Subscribe(topic string, rebalanceCb lib.RebalanceCb) error
	SubscribeTopics(topics []string, rebalanceCb lib.RebalanceCb) (err error)
	Subscription() (topics []string, err error)
	Unassign() (err error)
	Unsubscribe() (err error)
}
