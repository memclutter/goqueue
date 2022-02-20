package goqueue

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Callback func(amqp.Delivery, *log.Entry) (Retry, error)
