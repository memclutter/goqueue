package goqueue

import (
	"fmt"
	"github.com/streadway/amqp"
)

func (c DefaultConsumer) declare() error {
	if err := c.amqpCh.Qos(1, 0, false); err != nil {
		return fmt.Errorf("error set qos: %v", err)
	}

	exchange := getExchangeName(c.name, -1)
	queue := getQueueName(c.name, -1, nil)

	c.log.Infof("declare exchange: %s", exchange)
	if err := c.amqpCh.ExchangeDeclare(
		exchange,
		amqp.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil); err != nil {
		return err
	}

	c.log.Infof("declare name: %s", queue)
	if _, err := c.amqpCh.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil); err != nil {
		return err
	} else if err := c.amqpCh.QueueBind(
		queue,
		queue,
		exchange,
		false,
		nil); err != nil {
		return err
	}

	if len(c.retryIntervals) > 0 {

		exchangeRetry := getExchangeName(c.name, 0)

		c.log.Infof("declare excahnge: %s", exchangeRetry)
		if err := c.amqpCh.ExchangeDeclare(
			exchangeRetry,
			amqp.ExchangeDirect,
			true,
			false,
			false,
			false,
			nil); err != nil {
			return err
		}

		for retry, interval := range c.retryIntervals {
			queueRetry := getQueueName(c.name, retry, c.retryIntervals)

			c.log.Infof("declare name: %s", queueRetry)
			if _, err := c.amqpCh.QueueDeclare(
				queueRetry,
				true,
				false,
				false,
				false,
				amqp.Table{
					"x-dead-letter-exchange":    exchange,
					"x-dead-letter-routing-key": queue,
					"x-message-ttl":             interval.Milliseconds(),
				}); err != nil {
				return err
			} else if err := c.amqpCh.QueueBind(
				queueRetry,
				queueRetry,
				exchangeRetry,
				false,
				nil); err != nil {
				return err
			}
		}
	}

	return nil
}
