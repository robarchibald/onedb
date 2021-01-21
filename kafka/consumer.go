package kafka

import (
	"time"

	"github.com/stretchr/testify/mock"
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

// MockConsumer is a mock of the Consumer interface using github.com/stretchr/testify/mock
type MockConsumer struct {
	mock.Mock
}

// NewMockConsumer returns a new MockConsumer struct
func NewMockConsumer() *MockConsumer {
	return &MockConsumer{}
}

// Assign method
func (c *MockConsumer) Assign(partitions []lib.TopicPartition) (err error) {
	return c.Called(partitions).Error(0)
}

// Assignment method
func (c *MockConsumer) Assignment() ([]lib.TopicPartition, error) {
	res := c.Called()
	partitions, err := res.Get(0), res.Error(1)
	if partitions != nil {
		return partitions.([]lib.TopicPartition), err
	}
	return nil, err
}

// Close method
func (c *MockConsumer) Close() (err error) {
	return c.Called().Error(0)
}

// Commit method
func (c *MockConsumer) Commit() ([]lib.TopicPartition, error) {
	res := c.Called()
	partitions, err := res.Get(0), res.Error(1)
	if partitions != nil {
		return partitions.([]lib.TopicPartition), err
	}
	return nil, err
}

// CommitMessage method
func (c *MockConsumer) CommitMessage(m *lib.Message) ([]lib.TopicPartition, error) {
	res := c.Called(m)
	partitions, err := res.Get(0), res.Error(1)
	if partitions != nil {
		return partitions.([]lib.TopicPartition), err
	}
	return nil, err
}

// CommitOffsets method
func (c *MockConsumer) CommitOffsets(offsets []lib.TopicPartition) ([]lib.TopicPartition, error) {
	res := c.Called(offsets)
	partitions, err := res.Get(0), res.Error(1)
	if partitions != nil {
		return partitions.([]lib.TopicPartition), err
	}
	return nil, err
}

// Committed method
func (c *MockConsumer) Committed(partitions []lib.TopicPartition, timeoutMs int) ([]lib.TopicPartition, error) {
	res := c.Called(partitions, timeoutMs)
	offsets, err := res.Get(0), res.Error(1)
	if offsets != nil {
		return offsets.([]lib.TopicPartition), err
	}
	return nil, err
}

// Events method
func (c *MockConsumer) Events() chan lib.Event {
	res := c.Called().Get(0)
	if res != nil {
		return res.(chan lib.Event)
	}
	return nil
}

// GetConsumerGroupMetadata method
func (c *MockConsumer) GetConsumerGroupMetadata() (*lib.ConsumerGroupMetadata, error) {
	res := c.Called()
	m, err := res.Get(0), res.Error(1)
	if m != nil {
		return m.(*lib.ConsumerGroupMetadata), err
	}
	return nil, err
}

// GetMetadata method
func (c *MockConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*lib.Metadata, error) {
	res := c.Called(topic, allTopics, timeoutMs)
	m, err := res.Get(0), res.Error(1)
	if m != nil {
		return m.(*lib.Metadata), err
	}
	return nil, err
}

// GetWatermarkOffsets method
func (c *MockConsumer) GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error) {
	res := c.Called(topic, partition)
	return int64(res.Int(0)), int64(res.Int(1)), res.Error(2)
}

// Logs method
func (c *MockConsumer) Logs() chan lib.LogEvent {
	res := c.Called()
	e := res.Get(0)
	if e != nil {
		return e.(chan lib.LogEvent)
	}
	return nil
}

// OffsetsForTimes method
func (c *MockConsumer) OffsetsForTimes(times []lib.TopicPartition, timeoutMs int) (offsets []lib.TopicPartition, err error) {
	res := c.Called(times, timeoutMs)
	o, err := res.Get(0), res.Error(1)
	if o != nil {
		return o.([]lib.TopicPartition), err
	}
	return nil, err
}

// Pause method
func (c *MockConsumer) Pause(partitions []lib.TopicPartition) (err error) {
	return c.Called(partitions).Error(0)
}

// Poll method
func (c *MockConsumer) Poll(timeoutMs int) (event lib.Event) {
	res := c.Called(timeoutMs)
	e := res.Get(0)
	if e != nil {
		return e.(lib.Event)
	}
	return nil
}

// Position method
func (c *MockConsumer) Position(partitions []lib.TopicPartition) (offsets []lib.TopicPartition, err error) {
	res := c.Called(partitions)
	m, err := res.Get(0), res.Error(1)
	if m != nil {
		return m.([]lib.TopicPartition), err
	}
	return nil, err
}

// QueryWatermarkOffsets method
func (c *MockConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	res := c.Called(topic, partition, timeoutMs)
	return int64(res.Int(0)), int64(res.Int(1)), res.Error(2)
}

// ReadMessage method
func (c *MockConsumer) ReadMessage(timeout time.Duration) (*lib.Message, error) {
	res := c.Called(timeout)
	m, err := res.Get(0), res.Error(1)
	if m != nil {
		return m.(*lib.Message), err
	}
	return nil, err
}

// Resume method
func (c *MockConsumer) Resume(partitions []lib.TopicPartition) (err error) {
	return c.Called(partitions).Error(0)
}

// Seek method
func (c *MockConsumer) Seek(partition lib.TopicPartition, timeoutMs int) error {
	return c.Called(partition, timeoutMs).Error(0)
}

// SetOAuthBearerToken method
func (c *MockConsumer) SetOAuthBearerToken(oauthBearerToken lib.OAuthBearerToken) error {
	return c.Called(oauthBearerToken).Error(0)
}

// SetOAuthBearerTokenFailure method
func (c *MockConsumer) SetOAuthBearerTokenFailure(errstr string) error {
	return c.Called(errstr).Error(0)
}

// StoreOffsets method
func (c *MockConsumer) StoreOffsets(offsets []lib.TopicPartition) ([]lib.TopicPartition, error) {
	res := c.Called(offsets)
	o, err := res.Get(0), res.Error(1)
	if o != nil {
		return o.([]lib.TopicPartition), err
	}
	return nil, err
}

// String method
func (c *MockConsumer) String() string {
	return c.Called().String(0)
}

// Subscribe method
func (c *MockConsumer) Subscribe(topic string, rebalanceCb lib.RebalanceCb) error {
	return c.Called(topic, rebalanceCb).Error(0)
}

// SubscribeTopics method
func (c *MockConsumer) SubscribeTopics(topics []string, rebalanceCb lib.RebalanceCb) (err error) {
	return c.Called(topics, rebalanceCb).Error(0)
}

// Subscription method
func (c *MockConsumer) Subscription() (topics []string, err error) {
	res := c.Called(topics)
	t, err := res.Get(0), res.Error(1)
	if t != nil {
		return t.([]string), err
	}
	return nil, err
}

// Unassign method
func (c *MockConsumer) Unassign() (err error) {
	return c.Called().Error(0)
}

// Unsubscribe method
func (c *MockConsumer) Unsubscribe() (err error) {
	return c.Called().Error(0)
}
