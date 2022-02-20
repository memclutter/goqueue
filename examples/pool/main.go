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

	// Start 10 consumer workers
	goqueue.NewPool("tasks", 10, nil, func(delivery amqp.Delivery, entry *log.Entry) (goqueue.Retry, error) {
		time.Sleep(1 * time.Second)
		// Do some work
		return goqueue.RetryIgnore, nil
	}, amqpConn, nil).Start()
}
