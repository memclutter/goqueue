package goqueue

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type Consumer interface {
	Consume() error
}

func NewConsumer(queue string, retryIntervals []time.Duration, callback Callback, amqpConn *amqp.Connection, consumerLog *log.Entry) Consumer {
	if consumerLog == nil {
		consumerLog = log.WithFields(log.Fields{"_default": true})
	}
	consumerLog = consumerLog.WithFields(log.Fields{"queue": queue})

	return &DefaultConsumer{
		queue:          queue,
		retryIntervals: retryIntervals,
		callback:       callback,
		amqpConn:       amqpConn,
		log:            consumerLog,
	}
}

type DefaultConsumer struct {
	queue          string
	retryIntervals []time.Duration
	callback       Callback
	amqpConn       *amqp.Connection
	amqpCh         *amqp.Channel
	log            *log.Entry
}

func (c DefaultConsumer) Consume() error {

	var err error

	// Open channel in rabbitmq
	c.amqpCh, err = c.amqpConn.Channel()
	if err != nil {
		return fmt.Errorf("open channel error: %v", err)
	}
	defer func() {
		if err := c.amqpCh.Close(); err != nil {
			c.log.Errorf("close channel error (ignore): %v", err)
		}
	}()

	// Declare exchanges, queues, set qos etc
	if err = c.declare(); err != nil {
		return fmt.Errorf("declare error: %v", err)
	}

	// Create co channel for consume deliveries
	deliveries, err := c.amqpCh.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error create consume channel: %v", err)
	}

	// Process deliveries
	for delivery := range deliveries {

		// How more this delivery retry
		retry := getRetry(delivery.Headers)

		// Context logger for delivery
		deliveryLog := c.log.WithFields(log.Fields{
			"deliveryTag": delivery.DeliveryTag,
			"retry":       retry,
		})
		deliveryLog.Infof("receive delivery")
		deliveryLog.Debugf("body: %s", delivery.Body)

		// Process with retries logic
		c.ProcessDelivery(delivery, deliveryLog, retry)
	}

	return nil
}

func (c DefaultConsumer) ProcessDelivery(delivery amqp.Delivery, deliveryLog *log.Entry, retry int) {
	defer func() {
		if err := delivery.Ack(false); err != nil {
			deliveryLog.Errorf("ack error (ignored): %v", err)
		}
	}()

	// Callback for user code to process delivery. If error, check retryStatus
	// - RetryNext - redelivery this delivery for retry after some intervals
	// - RetryStop - ignore
	retryStatus, err := c.callback(delivery, deliveryLog)
	if err != nil {
		deliveryLog.Warnf("callback error: %v", err)
		switch retryStatus {
		case RetryNext:
			queueRetry := getQueueName(c.queue, retry, c.retryIntervals)
			if len(queueRetry) == 0 {
				deliveryLog.Infof("retry finish")
				return
			}

			deliveryLog.Infof("retry next")

			headers := delivery.Headers
			if headers == nil {
				headers = amqp.Table{}
			}
			headers[RetryHeader] = retry + 1

			if err := Publish(c.amqpCh, queueRetry, delivery.Body, headers); err != nil {
				deliveryLog.Errorf("error retry: %v", err)
			}
		case RetryStop:
			deliveryLog.Infof("retry stop")
		default:
			deliveryLog.Warnf("retry ignore")
		}
	}
}
