package goqueue

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type Pool interface {
	Start()
}

func NewPool(queue string, workers int, retryIntervals []time.Duration, callback Callback, amqpConn *amqp.Connection, poolLog *log.Entry) Pool {
	if poolLog == nil {
		poolLog = log.WithField("_default", true)
	}

	return &DefaultPool{
		queue:          queue,
		workers:        workers,
		retryIntervals: retryIntervals,
		callback:       callback,
		amqpConn:       amqpConn,
		log: log.WithFields(log.Fields{
			"name": queue,
		}),
	}
}

type DefaultPool struct {
	queue          string
	workers        int
	retryIntervals []time.Duration
	callback       Callback
	amqpConn       *amqp.Connection
	log            *log.Entry
}

func (p DefaultPool) Start() {
	var wg sync.WaitGroup
	wg.Add(p.workers)
	p.log.Infof("start pool")

	for i := 0; i < p.workers; i++ {
		go func(workerNum int) {
			defer wg.Done()

			consumerLog := p.log.WithField("worker", workerNum)
			consumerLog.Infof("start worker")
			consumer := NewConsumer(p.queue, p.retryIntervals, p.callback, p.amqpConn, consumerLog)
			if err := consumer.Consume(); err != nil {
				consumerLog.Errorf("error run worker: %v", err)
				return
			}

			consumerLog.Infof("stop worker")
		}(i)
	}

	wg.Wait()

	p.log.Infof("stop pool")
}
