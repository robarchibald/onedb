package kafka

import (
	"fmt"

	lib "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// NewAdminClientFromProducer derives a new AdminClient from an existing Producer instance.
// The AdminClient will use the same configuration and connections as the parent instance.
func NewAdminClientFromProducer(p Producer) (a *lib.AdminClient, err error) {
	if pl, ok := p.(*lib.Producer); ok {
		return lib.NewAdminClientFromProducer(pl)
	}
	return nil, fmt.Errorf("unable to create admin client from mock producer")
}

// NewAdminClientFromConsumer derives a new AdminClient from an existing Consumer instance.
// The AdminClient will use the same configuration and connections as the parent instance.
func NewAdminClientFromConsumer(c Consumer) (a *lib.AdminClient, err error) {
	if cl, ok := c.(*lib.Consumer); ok {
		return lib.NewAdminClientFromConsumer(cl)
	}
	return nil, fmt.Errorf("unable to create admin client from mock consumer")
}
