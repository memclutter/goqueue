package goqueue

type Retry int

const (
	RetryNext = iota - 3
	RetryStop
	RetryIgnore
)
