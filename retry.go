package goqueue

type Retry int

const (
	RetryNext = iota
	RetryStop
	RetryIgnore
)
