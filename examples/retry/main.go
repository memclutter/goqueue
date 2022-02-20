package main

import (
	"github.com/memclutter/goqueue"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

func main() {
	amqpConn, err := amqp.Dial("amqp://localhost/")
	if err != nil {
		log.Fatalln(err)
	}

	retryIntervals := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}

	if err := goqueue.NewConsumer("tasks", retryIntervals, func(delivery amqp.Delivery, entry *log.Entry) (goqueue.Retry, error) {
		time.Sleep(1 * time.Second)
		// Do some work
		return goqueue.RetryNext, nil
	}, amqpConn, nil).Consume(); err != nil {
		log.Fatalln(err)
	}
}
