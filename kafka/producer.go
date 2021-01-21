package kafka

import (
	"context"

	"github.com/stretchr/testify/mock"
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

// MockProducer is a mock of the Producer interface using github.com/stretchr/testify/mock
type MockProducer struct {
	mock.Mock
}

// NewMockProducer returns a new MockProducer struct
func NewMockProducer() *MockProducer {
	return &MockProducer{}
}

// AbortTransaction method
func (p *MockProducer) AbortTransaction(ctx context.Context) error {
	return p.Called(ctx).Error(0)
}

// BeginTransaction method
func (p *MockProducer) BeginTransaction() error {
	return p.Called().Error(0)
}

// Close emethod
func (p *MockProducer) Close() {
	p.Called()
}

// CommitTransaction method
func (p *MockProducer) CommitTransaction(ctx context.Context) error {
	return p.Called(ctx).Error(0)
}

// Events method
func (p *MockProducer) Events() chan lib.Event {
	res := p.Called().Get(0)
	if res != nil {
		return res.(chan lib.Event)
	}
	return nil
}

// Flush method
func (p *MockProducer) Flush(timeoutMs int) int {
	return p.Called(timeoutMs).Int(0)
}

// GetFatalError method
func (p *MockProducer) GetFatalError() error {
	return p.Called().Error(0)
}

// GetMetadata method
func (p *MockProducer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*lib.Metadata, error) {
	res := p.Called(topic, allTopics, timeoutMs)
	m, err := res.Get(0), res.Error(1)
	if m != nil {
		return m.(*lib.Metadata), err
	}
	return nil, err
}

// InitTransactions method
func (p *MockProducer) InitTransactions(ctx context.Context) error {
	return p.Called(ctx).Error(0)
}

// Len method
func (p *MockProducer) Len() int {
	return p.Called().Int(0)
}

// Logs method
func (p *MockProducer) Logs() chan lib.LogEvent {
	res := p.Called().Get(0)
	if res != nil {
		return res.(chan lib.LogEvent)
	}
	return nil
}

// OffsetsForTimes method
func (p *MockProducer) OffsetsForTimes(times []lib.TopicPartition, timeoutMs int) ([]lib.TopicPartition, error) {
	res := p.Called(times, timeoutMs)
	offsets, err := res.Get(0), res.Error(1)
	if offsets != nil {
		return offsets.([]lib.TopicPartition), err
	}
	return nil, err
}

// Produce method
func (p *MockProducer) Produce(msg *lib.Message, deliveryChan chan lib.Event) error {
	return p.Called(msg, deliveryChan).Error(0)
}

// ProduceChannel method
func (p *MockProducer) ProduceChannel() chan *lib.Message {
	c := p.Called().Get(0)
	if c != nil {
		return c.(chan *lib.Message)
	}
	return nil
}

// Purge method
func (p *MockProducer) Purge(flags int) error {
	return p.Called(flags).Error(0)
}

// QueryWatermarkOffsets method
func (p *MockProducer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	res := p.Called(topic, partition, timeoutMs)
	return int64(res.Int(0)), int64(res.Int(1)), res.Error(2)
}

// SendOffsetsToTransaction method
func (p *MockProducer) SendOffsetsToTransaction(ctx context.Context, offsets []lib.TopicPartition, consumerMetadata *lib.ConsumerGroupMetadata) error {
	return p.Called(ctx, offsets, consumerMetadata).Error(0)
}

// SetOAuthBearerToken method
func (p *MockProducer) SetOAuthBearerToken(oauthBearerToken lib.OAuthBearerToken) error {
	return p.Called(oauthBearerToken).Error(0)
}

// SetOAuthBearerTokenFailure method
func (p *MockProducer) SetOAuthBearerTokenFailure(errstr string) error {
	return p.Called(errstr).Error(0)
}

// String method
func (p *MockProducer) String() string {
	return p.Called().String(0)
}

// TestFatalError method
func (p *MockProducer) TestFatalError(code lib.ErrorCode, str string) lib.ErrorCode {
	return p.Called(code, str).Get(0).(lib.ErrorCode)
}
