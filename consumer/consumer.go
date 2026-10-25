package consumer

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

const (
	// An event of this type should be sent to the message events channel
	// when the message is offered to a client.
	EvOffered eventType = iota

	// An event of this type should be sent to the message events channel
	// when the message is acknowledged by a client.
	EvAcked
)

var (
	ErrRequestTimeout = errors.New("long polling timeout")
	// ErrRequestCanceled is returned when a consume request is canceled by its
	// caller (e.g. gRPC stream context or HTTP client disconnect) before a
	// message handoff is committed.
	ErrRequestCanceled = errors.New("consume request canceled")
	// ErrRequestDeadline is returned when the caller-supplied deadline on a
	// consume request expires before a message handoff is committed.
	ErrRequestDeadline = errors.New("consume request deadline exceeded")
	ErrUnavailable     = errors.New("service is shutting down")
	ErrTooManyRequests = errors.New("Too many requests. Consider increasing `consumer.channel_buffer_size` (https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L43)")
)

type T interface {
	// Consume consumes a message from the specified topic on behalf of the
	// specified consumer group. If there are no more new messages in the topic
	// at the time of the request then it will block for
	// `Config.Consumer.LongPollingTimeout`. If no new message is produced during
	// that time, then `ErrRequestTimeout` is returned.
	//
	// Note that during state transitions topic subscribe<->unsubscribe and
	// consumer group register<->deregister the method may return either
	// `ErrBufferOverflow` or `ErrRequestTimeout` even when there are messages
	// available for consumption. In that case the user should back off a bit
	// and then repeat the request.
	Consume(group, topic string) (Message, error)

	// ConsumeCtx is a context-aware counterpart of Consume. If the context is
	// canceled or its deadline expires before a message is handed off, the
	// request is terminated with ErrRequestCanceled/ErrRequestDeadline and no
	// message is offered or auto-acknowledged on its behalf.
	ConsumeCtx(ctx context.Context, group, topic string) (Message, error)

	// AsyncConsume is an asynchronous counterpart of Consume function. It
	// sends a response down to a buffered channel of the consumer machinery
	// and returns a channel that a response should be expected from.
	AsyncConsume(group, topic string) <-chan Response

	// AsyncConsumeCtx is a context-aware counterpart of AsyncConsume. The
	// request is finalized exactly once with either a message, a long polling
	// timeout, a cancellation/deadline error, or a shutdown/overflow error.
	AsyncConsumeCtx(ctx context.Context, group, topic string) <-chan Response

	// Stop sends a shutdown signal to all internal goroutines and blocks until
	// they are stopped. It is guaranteed that all last consumed offsets of all
	// consumer groups/topics are committed to Kafka before Consumer stops.
	Stop()
}

// Request
type Request struct {
	Timestamp  time.Time
	Group      string
	Topic      string
	ResponseCh chan Response
	ctx        context.Context
}

// Context returns the cancellation/deadline context associated with the
// request. Requests created without an explicit context are associated with
// context.Background, therefore the returned value is never nil.
func (rq Request) Context() context.Context {
	if rq.ctx == nil {
		return context.Background()
	}
	return rq.ctx
}

// Response defines responses returned upstream by the children.
type Response struct {
	Msg Message
	Err error
}

// Message encapsulates a Kafka message returned by the consumer.
type Message struct {
	sarama.ConsumerMessage
	HighWaterMark int64
	EventsCh      chan<- Event

	// ResetCh is an INTERNAL per-record, capacity-1 cancellation signal. It
	// is created by the partition consumer for each fetched record and sent
	// one empty value by the topic consumer if a request that reserved the
	// record loses to cancellation before the handoff arbitration point.
	// The dedicated channel does not contend with acknowledgment events, a
	// non-blocking send never blocks the topic consumer (a queued signal
	// already means redelivery is armed; with a stopped partition consumer
	// the send simply drops), and it can fire again for the same record if
	// it is reserved and canceled once more after redelivery. The record
	// itself was never offered, tracked or acked, so if its partition
	// consumer is gone the next owner redelivers it from the last committed
	// offset.
	ResetCh chan struct{}
}

func NewRequest(group, topic string) Request {
	return NewRequestCtx(context.Background(), group, topic)
}

// NewRequestCtx creates a consume request bound to the specified context. A
// nil context is treated as context.Background to stay compatible with
// internal callers that do not carry a context.
func NewRequestCtx(ctx context.Context, group, topic string) Request {
	if ctx == nil {
		ctx = context.Background()
	}
	return Request{
		Timestamp:  time.Now().UTC(),
		Group:      group,
		Topic:      topic,
		ResponseCh: make(chan Response, 1),
		ctx:        ctx,
	}
}

// CanceledResponse builds the terminal response for a request whose context is
// done. A deadline expiration maps to ErrRequestDeadline, any other context
// error maps to ErrRequestCanceled.
func CanceledResponse(ctxErr error) Response {
	if ctxErr == context.DeadlineExceeded {
		return Response{Err: ErrRequestDeadline}
	}
	return Response{Err: ErrRequestCanceled}
}

func Ack(offset int64) Event {
	return Event{EvAcked, offset}
}

type Event struct {
	T      eventType
	Offset int64
}

type eventType int
