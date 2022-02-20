package goqueue

import (
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_getRetry(t *testing.T) {
	subtests := []struct {
		title   string
		headers amqp.Table
		retry   int
	}{
		{
			title: "Can get correct int retry",
			headers: amqp.Table{
				RetryHeader: 3,
			},
			retry: 3,
		},
		{
			title: "Can get correct int32 retry",
			headers: amqp.Table{
				RetryHeader: int32(4),
			},
			retry: 4,
		},
		{
			title: "Can get correct int64 retry",
			headers: amqp.Table{
				RetryHeader: int64(5),
			},
			retry: 5,
		},
		{
			title: "Can get default zero for unsupported type",
			headers: amqp.Table{
				RetryHeader: "20",
			},
			retry: 0,
		},
		{
			title:   "Can get zero retry",
			headers: amqp.Table{},
			retry:   0,
		},
	}

	for _, subtest := range subtests {
		t.Run(subtest.title, func(t *testing.T) {
			assert.Equal(t, subtest.retry, getRetry(subtest.headers))
		})
	}
}

func Test_getExchangeName(t *testing.T) {
	subtests := []struct {
		title    string
		queue    string
		retry    int
		exchange string
	}{
		{
			title:    "Can get name",
			queue:    "test",
			retry:    -1,
			exchange: "test",
		},
		{
			title:    "Can get retry name",
			queue:    "test",
			retry:    20,
			exchange: "test__retry",
		},
	}

	for _, subtest := range subtests {
		t.Run(subtest.title, func(t *testing.T) {
			assert.Equal(t, subtest.exchange, getExchangeName(subtest.queue, subtest.retry))
		})
	}
}

func Test_getQueueName(t *testing.T) {
	subtests := []struct {
		title     string
		queue     string
		retry     int
		intervals []time.Duration
		queueName string
	}{
		{
			title:     "Can get name",
			queue:     "test",
			retry:     -1,
			intervals: []time.Duration{},
			queueName: "test",
		},
		{
			title: "Can get retry name",
			queue: "test",
			retry: 2,
			intervals: []time.Duration{
				1 * time.Second,
				5 * time.Second,
				10 * time.Second,
			},
			queueName: "test__retry__10s",
		},
		{
			title:     "Can get retry overflow name",
			queue:     "test",
			retry:     10,
			intervals: []time.Duration{},
			queueName: "",
		},
	}

	for _, subtest := range subtests {
		t.Run(subtest.title, func(t *testing.T) {
			assert.Equal(t, subtest.queueName, getQueueName(subtest.queue, subtest.retry, subtest.intervals))
		})
	}
}