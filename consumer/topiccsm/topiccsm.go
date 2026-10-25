package topiccsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/holster/v4/clock"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
)

var (
	requestTimeoutRs         = consumer.Response{Err: consumer.ErrRequestTimeout}
	requestDeadlineRs        = consumer.Response{Err: consumer.ErrRequestDeadline}
	safe2StopPollingInterval = 100 * time.Millisecond
)

// ReserveBarrier is a TESTING ONLY synchronization handle. When AfterReserveCh
// is non-nil, the topic consumer blocks after a message has been reserved for
// a request (taken from the multiplexer, but not offered yet) and before the
// unique cancellation/handoff arbitration point, until the test receives the
// barrier from the channel and calls Release.
type ReserveBarrier struct {
	Msg     consumer.Message
	release chan struct{}
}

// Release lets the topic consumer proceed to the cancellation/handoff
// arbitration point. Calling it more than once panics.
func (b *ReserveBarrier) Release() {
	close(b.release)
}

// AfterReserveCh is TESTING ONLY, leave it nil in production.
var AfterReserveCh chan *ReserveBarrier

// T serves consume requests received from childSpec.Requests() channel with
// messages received from Messages() channel. The topic consumer expires and
// shuts itself down when either of the following happens:
//   - there has been no requests for Consumer.SubscriptionTimeout and
//     isSafe2StopFn returns true;
//   - there has been no requests for max value of Consumer.SubscriptionTimeout
//     and Consumer.AckTimeout
//
// implements `multiplexer.Out`.
type T struct {
	actDesc       *actor.Descriptor
	childSpec     dispatcher.ChildSpec
	cfg           *config.Proxy
	group         string
	topic         string
	lifespanCh    chan<- *T
	isSafe2StopFn func() bool
	messagesCh    chan consumer.Message
	wg            sync.WaitGroup
}

// Spawn creates and starts a topic consumer instance.
func Spawn(parentActDesc *actor.Descriptor, group string, childSpec dispatcher.ChildSpec,
	cfg *config.Proxy, lifespanCh chan<- *T, isSafe2StopFn func() bool,
) *T {
	topic := string(childSpec.Key())
	actDesc := parentActDesc.NewChild(fmt.Sprintf("%s", topic))
	actDesc.AddLogField("kafka.group", group)
	actDesc.AddLogField("kafka.topic", topic)
	tc := T{
		actDesc:       actDesc,
		childSpec:     childSpec,
		cfg:           cfg,
		group:         group,
		topic:         topic,
		lifespanCh:    lifespanCh,
		isSafe2StopFn: isSafe2StopFn,

		// Messages channel must be non-buffered. Otherwise we might end up
		// buffering a message from a partition that no longer belongs to this
		// consumer group member.
		messagesCh: make(chan consumer.Message),
	}
	actor.Spawn(tc.actDesc, &tc.wg, tc.run)
	return &tc
}

// Topic returns the topic name this topic consumer is responsible for.
func (tc *T) Topic() string {
	return tc.topic
}

// implements `multiplexer.Out`
func (tc *T) Messages() chan<- consumer.Message {
	return tc.messagesCh
}

func (tc *T) run() {
	defer tc.childSpec.Dispose()
	tc.lifespanCh <- tc
	defer func() {
		tc.lifespanCh <- tc
	}()

	latestRqTime := clock.Now().UTC()
	expireTimer := clock.NewTimer(tc.cfg.Consumer.SubscriptionTimeout)
	defer expireTimer.Stop()
	for {
		// Serve requests, and jump to the safe stop when they cease coming.
	serveRequests:
		for {
			select {
			case consumeRq, ok := <-tc.childSpec.Requests():
				if !ok {
					tc.actDesc.Log().Info("Shutting down")
					return
				}
				if rqTime, live := tc.serveRequest(consumeRq); live {
					latestRqTime = rqTime
				}
			case <-expireTimer.C():
				sinceLatestRq := clock.Now().UTC().Sub(latestRqTime)
				subscriptionTTL := tc.cfg.Consumer.SubscriptionTimeout - sinceLatestRq
				if subscriptionTTL <= 0 {
					tc.actDesc.Log().Info("Topic subscription expired")
					goto wait4SafeStop
				}
				expireTimer.Reset(subscriptionTTL)
			}
		}
		// Keep polling isSafe2StopFn until it returns true or the ack timeout
		// expires and then terminate. If new request arrives while waiting,
		// then jump back to the request serving loop.
	wait4SafeStop:
		for {
			if tc.isSafe2StopFn() {
				return
			}
			sinceLatestRq := clock.Now().UTC().Sub(latestRqTime)
			if sinceLatestRq >= tc.cfg.Consumer.AckTimeout {
				tc.actDesc.Log().Error("Stopping unsafely, some messages can be consumed more than once")
				return
			}
			select {
			case consumeRq, ok := <-tc.childSpec.Requests():
				if !ok {
					tc.actDesc.Log().Info("Signaled to shutdown")
					return
				}
				// Canceled/deadline requests are not live requests: keep
				// winding down instead of reviving the subscription.
				rqTime, live := tc.serveRequest(consumeRq)
				if !live {
					continue
				}
				tc.actDesc.Log().Info("Resume request handling")
				latestRqTime = rqTime
				// This reset is merely a kick: the TTL is anchored to the
				// loop-top time, so the timer is usually scheduled in the
				// past and fires right away. The expire branch above then
				// recomputes the TTL against the refreshed latestRqTime and
				// re-arms the timer properly. Anchoring to the loop-top time
				// (instead of a full timeout from now) also makes the
				// scheduling immune to the time-advance/run-goroutine
				// ordering race under the frozen test clock.
				subscriptionTTL := tc.cfg.Consumer.SubscriptionTimeout - sinceLatestRq
				expireTimer.Reset(subscriptionTTL)
				goto serveRequests
			case <-clock.After(safe2StopPollingInterval):
			}
		}
	}
}

func (tc *T) String() string {
	return tc.actDesc.String()
}

// serveRequest serves a single consume request. It distinguishes four
// outcomes:
//
//  1. queuing      — the request waits for a message up to its long polling /
//     deadline TTL;
//  2. reservation  — a message is taken from the multiplexer but not offered
//     yet;
//  3. handoff      — the unique arbitration point sees a live context, so
//     EvOffered is emitted and the message response is sent;
//  4. cancellation — the context is done before the arbitration point commits
//     the handoff. The request is finalized exactly once with
//     ErrRequestCanceled/ErrRequestDeadline, it does not count
//     as a valid consumption and the reserved record is
//     returned to its partition consumer (never offered, never
//     tracked) for immediate redelivery to the next live
//     request.
//
// It returns the request handling time, captured at entry, and true only if
// the request counted as subscription activity (a delivered handoff or an
// ordinary long polling timeout). Canceled and deadline-expired requests do
// not refresh the subscription active time. The time is captured at entry,
// before any blocking rendezvous, so that it cannot be skewed by a clock
// advance that happens while the call is in progress.
func (tc *T) serveRequest(consumeRq consumer.Request) (time.Time, bool) {
	latestRqTime := clock.Now().UTC()
	ctx := consumeRq.Context()

	// Already terminal requests are dropped before anything is reserved for
	// them. The response channel is finalized exactly once.
	if err := ctx.Err(); err != nil {
		tc.actDesc.Log().Warnf("Canceled request: %+v, err=%v", consumeRq, err)
		consumeRq.ResponseCh <- consumer.CanceledResponse(err)
		return latestRqTime, false
	}

	requestAge := latestRqTime.Sub(consumeRq.Timestamp)
	requestTTL := tc.cfg.Consumer.LongPollingTimeout - requestAge
	// The request has been waiting in the buffer for too long. If we
	// reply with a fetched message, then there is a good chance that the
	// client won't receive it due to the client HTTP timeout. Therefore
	// we reject the request to avoid message loss.
	if requestTTL <= 0 {
		consumeRq.ResponseCh <- requestTimeoutRs
		return latestRqTime, true
	}

	waitTTL := requestTTL
	deadlineCapped := false
	// A caller-supplied deadline shorter than the long polling TTL caps the
	// wait. Real wall clock is used so that context deadline and timer
	// measure the same time.
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := deadline.Sub(time.Now()); remaining < waitTTL {
			waitTTL = remaining
			deadlineCapped = true
		}
	}
	waitTimer := clock.NewTimer(waitTTL)
	defer waitTimer.Stop()
	var msg consumer.Message
	select {
	case msg = <-tc.messagesCh:
	case <-ctx.Done():
		// Cancellation and message availability may both be ready here.
		// There is a single arbitration point below, therefore give an
		// already-arriving message one last chance to be reserved. If no
		// message is arriving at this instant then cancellation wins and any
		// later message is naturally picked up by the next request.
		select {
		case msg = <-tc.messagesCh:
		default:
			consumeRq.ResponseCh <- consumer.CanceledResponse(ctx.Err())
			return latestRqTime, false
		}
	case <-waitTimer.C():
		if deadlineCapped {
			consumeRq.ResponseCh <- requestDeadlineRs
			return latestRqTime, false
		}
		consumeRq.ResponseCh <- requestTimeoutRs
		return latestRqTime, true
	}

	// TESTING ONLY: pause between reservation and the arbitration point so
	// that tests can deterministically trigger cancellation with a record
	// reserved but not offered.
	if AfterReserveCh != nil {
		barrier := &ReserveBarrier{Msg: msg, release: make(chan struct{})}
		AfterReserveCh <- barrier
		<-barrier.release
	}

	// >>> THE unique arbitration point. <<<
	//
	// The record is reserved (rendezvous with the multiplexer done, but the
	// partition consumer has not seen EvOffered, so its offset is neither
	// tracked nor eligible for acknowledgment). If the caller context is done
	// here, cancellation wins: the request does not count as a valid
	// consumption, does not refresh the subscription activity, and no
	// acknowledgment is possible for it. The reserved record is returned to
	// its partition consumer for redelivery, so the next live request
	// receives it immediately without waiting for ack_timeout and
	// GetOfferts/commits cannot skip its offset.
	if err := ctx.Err(); err != nil {
		tc.actDesc.Log().Warnf("Canceled after reservation: %+v, offset=%d, err=%v",
			consumeRq, msg.Offset, err)
		tc.releaseReservation(msg)
		consumeRq.ResponseCh <- consumer.CanceledResponse(err)
		return latestRqTime, false
	}

	// Handoff committed. From here on the standard noAck/explicit Ack/autoAck
	// rules apply; in particular autoAck may still happen before the network
	// response reaches the client (pre-existing semantics).
	msg.EventsCh <- consumer.Event{T: consumer.EvOffered, Offset: msg.Offset}
	consumeRq.ResponseCh <- consumer.Response{Msg: msg}
	return latestRqTime, true
}

// releaseReservation returns a reserved-but-never-offered record to its
// partition consumer, which is parked after handing the record to the
// multiplexer. The per-record signal channel has capacity 1 and is not shared
// with acknowledgment events, so the non-blocking send neither blocks the
// topic consumer nor can lose to a queued ack. If the send cannot be queued
// then a redelivery is already armed, or the partition consumer is stopping
// or gone (rebalance/topic expiry/shutdown): in the latter case the record
// was never offered, hence offerCount == 0 and the offset was never tracked
// or committed, so the partition's next owner redelivers it from the last
// committed offset. No record is ever left neither tracked nor
// redeliverable.
func (tc *T) releaseReservation(msg consumer.Message) {
	if msg.ResetCh == nil {
		return
	}
	select {
	case msg.ResetCh <- struct{}{}:
	default:
	}
}
