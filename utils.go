package goqueue

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

const RetryHeader = "x-goqueue-retry"

func getRetry(headers amqp.Table) int {

	// Default zero retry
	if headers == nil || len(headers) == 0 {
		return 0
	}

	switch retry := headers[RetryHeader].(type) {
	case int:
		return retry
	case int32:
		return int(retry)
	case int64:
		return int(retry)
	default:
		return 0
	}
}

func getExchangeName(queue string, retry int) string {
	if retry == -1 {
		return queue
	}

	return fmt.Sprintf("%s__retry", queue)
}

func getQueueName(queue string, retry int, intervals []time.Duration) string {
	if retry == -1 {
		return queue
	}

	if retry > len(intervals)-1 {
		return ""
	}

	return fmt.Sprintf("%s__retry__%ds", queue, int(intervals[retry].Seconds()))
}
