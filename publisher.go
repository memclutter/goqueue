package goqueue

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

func Publish(amqpCh *amqp.Channel, queue string, data interface{}, retry int) error {
	var err error

	exchange := getExchangeName(queue, -1)

	body, ok := data.([]byte)
	if !ok {
		body, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("error marshal json: %v", err)
		}
	}

	if err := amqpCh.Publish(exchange, queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Headers: amqp.Table{
			RetryHeader: retry + 1,
		},
	}); err != nil {
		return fmt.Errorf("error publish to amqp: %v", err)
	}

	return nil
}
