package goqueue

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

func Publish(amqpCh *amqp.Channel, consumerName string, data interface{}) error {
	var err error

	exchange := getExchangeName(consumerName, -1)

	body, ok := data.([]byte)
	if !ok {
		body, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("error marshal json: %v", err)
		}
	}

	if err := amqpCh.Publish(exchange, consumerName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return fmt.Errorf("error publish to amqp: %v", err)
	}

	return nil
}
