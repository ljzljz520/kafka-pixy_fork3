package topiccsm

import (
	"context"
	"runtime"
	"sync"
	"time"

	. "gopkg.in/check.v1"

	"github.com/mailgun/holster/v4/clock"
	"github.com/mailgun/kafka-pixy/consumer"
)

// cancelableCtx is a manually controlled context for deterministic
// cancellation/deadline tests that must not depend on real wall-clock sleeps.
type cancelableCtx struct {
	mu          sync.Mutex
	done        chan struct{}
	err         error
	deadline    time.Time
	hasDeadline bool
}

func newCancelableCtx() *cancelableCtx {
	return &cancelableCtx{done: make(chan struct{})}
}

func (x *cancelableCtx) withDeadline(d time.Duration) *cancelableCtx {
	x.mu.Lock()
	x.hasDeadline = true
	x.deadline = time.Now().Add(d)
	x.mu.Unlock()
	return x
}

func (x *cancelableCtx) Deadline() (time.Time, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.deadline, x.hasDeadline
}

func (x *cancelableCtx) Done() <-chan struct{} { return x.done }

func (x *cancelableCtx) Err() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.err
}

func (x *cancelableCtx) Value(key interface{}) interface{} {
	return nil
}

func (x *cancelableCtx) cancel() {
	x.finish(context.Canceled)
}

func (x *cancelableCtx) deadlineExceeded() {
	x.finish(context.DeadlineExceeded)
}

func (x *cancelableCtx) finish(err error) {
	x.mu.Lock()
	if x.err == nil {
		x.err = err
	}
	x.mu.Unlock()
	close(x.done)
}

// relayOutcome is the terminal observation of a partitionRelay delivery
// attempt.
type relayOutcome int

const (
	outcomeReset relayOutcome = iota
	outcomeOffered
)

// partitionRelay emulates the relevant partition consumer state machine for a
// single record: it delivers the message whenever delivery is armed, then
// waits for the outcome. Closing ResetCh arms another delivery (the record
// was never offered/tracked), EvOffered completes the relay. Every observed
// outcome is forwarded to observedCh.
type partitionRelay struct {
	msg        consumer.Message
	eventsCh   chan consumer.Event
	observedCh chan relayOutcome
	wg         sync.WaitGroup
}

func spawnPartitionRelay(tc *T, idx int) *partitionRelay {
	msg, eventsCh := newMessage(idx)
	r := &partitionRelay{
		msg:        msg,
		eventsCh:   eventsCh,
		observedCh: make(chan relayOutcome, 8),
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			tc.Messages() <- r.msg
			select {
			case e := <-r.eventsCh:
				if e.T != consumer.EvOffered || e.Offset != r.msg.Offset {
					r.observedCh <- outcomeReset
					continue
				}
				r.observedCh <- outcomeOffered
				return
			case <-r.msg.ResetCh:
				r.observedCh <- outcomeReset
			}
		}
	}()
	return r
}

// drainAfterStop unblocks a relay parked on the messages channel after the
// topic consumer run has stopped, then completes it with a synthetic offer so
// that the relay goroutine does not leak.
func (r *partitionRelay) drainAfterStop(tc *T) {
	<-tc.messagesCh
	r.eventsCh <- consumer.Event{T: consumer.EvOffered, Offset: r.msg.Offset}
	r.wg.Wait()
}

func (r *partitionRelay) assertObserved(c *C, want relayOutcome) {
	select {
	case got := <-r.observedCh:
		c.Assert(got, Equals, want)
	case <-time.After(time.Second):
		c.Errorf("Expected outcome %v, got none", want)
	}
}

func (r *partitionRelay) assertNoOffer(c *C) {
	for {
		select {
		case got := <-r.observedCh:
			if got == outcomeOffered {
				c.Errorf("Unexpected offer")
				return
			}
		default:
			return
		}
	}
}

// drainRelay consumes relay outcomes until the record's terminal offer and
// reports how many times the record was reset and offered.
func drainRelay(c *C, r *partitionRelay) (offers, resets int) {
	for {
		select {
		case got := <-r.observedCh:
			if got == outcomeOffered {
				return offers + 1, resets
			}
			resets++
		case <-time.After(2 * time.Second):
			c.Fatalf("relay record was never offered")
			return offers, resets
		}
	}
}

func newCtxRequest(ctx context.Context) consumer.Request {
	return consumer.NewRequestCtx(ctx, group, "t")
}

func assertRqResponse(c *C, rq consumer.Request, want consumer.Response, timeout time.Duration) {
	select {
	case got := <-rq.ResponseCh:
		c.Assert(got, DeepEquals, want)
	case <-time.After(timeout):
		c.Errorf("Response wait timeout, want=%v", want)
	}
}

func assertNoRqResponse(c *C, rq consumer.Request) {
	select {
	case rs := <-rq.ResponseCh:
		c.Errorf("Unexpected response: %v", rs)
	default:
	}
}

func spawnTestTc(s *TopicCsmSuite, c *C) *T {
	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)
	return tc
}

func stopTestTc(c *C, s *TopicCsmSuite, tc *T) {
	close(s.requestsCh)
	assertStopped(c, s.lifespanCh, time.Second*5)
}

// A request canceled before it reaches the topic consumer is rejected
// immediately and does not refresh the subscription activity time.
func (s *TopicCsmSuite) TestCancelQueued(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	spawnTestTc(s, c)

	ctx := newCancelableCtx()
	ctx.cancel()
	rq := newCtxRequest(ctx)

	// When
	s.requestsCh <- rq

	// Then: canceled on the spot.
	assertRqResponse(c, rq, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)

	// And: the topic consumer still expires at the original subscription
	// timeout, i.e. the canceled request did not count as activity.
	c.Assert(clock.Advance(500), Equals, time.Duration(500))
	assertStopped(c, s.lifespanCh, time.Second*5)
}

// When cancellation wins at the unique arbitration point (message reserved
// but not offered yet), the request gets exactly one canceled response, no
// EvOffered is emitted, and the reserved record is returned to its partition
// and handed to the next live request immediately -- without waiting for
// ack_timeout.
func (s *TopicCsmSuite) TestCancelAfterReservation(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = time.Hour
	tc := spawnTestTc(s, c)
	defer func() { stopTestTc(c, s, tc) }()

	barrierCh := make(chan *ReserveBarrier)
	AfterReserveCh = barrierCh
	defer func() { AfterReserveCh = nil }()

	ctx1 := newCancelableCtx()
	rq1 := newCtxRequest(ctx1)
	s.requestsCh <- rq1

	relay := spawnPartitionRelay(tc, 7)

	// Reservation made, handoff not committed yet.
	barrier := <-barrierCh
	c.Assert(barrier.Msg, DeepEquals, relay.msg)
	assertNoRqResponse(c, rq1)

	// When: the client cancels while the record is reserved.
	ctx1.cancel()
	barrier.Release()

	// Then: exactly one canceled response, and the record is reset rather
	// than offered.
	assertRqResponse(c, rq1, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)
	relay.assertObserved(c, outcomeReset)
	relay.assertNoOffer(c)

	// Disarm the reservation barrier before the next request reserves the
	// redelivered record.
	AfterReserveCh = nil

	// And: the next live request gets the very same record immediately.
	rq2 := newCtxRequest(context.Background())
	s.requestsCh <- rq2
	assertRqResponse(c, rq2, consumer.Response{Msg: relay.msg}, time.Second)
	relay.assertObserved(c, outcomeOffered)
	relay.wg.Wait()
}

// When cancellation and message availability race around the arbitration
// point, exactly one of them wins: if the handoff wins, the record is offered
// once to the racing request; if cancellation wins, the record is reset once
// and delivered to the next live request. Either way the record is offered
// exactly once overall, no request is finalized twice and no offset is
// skipped. The staggered delays exercise both select orders.
func (s *TopicCsmSuite) TestRaceCancelHandoffInvariants(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = time.Hour
	tc := spawnTestTc(s, c)
	defer func() { stopTestTc(c, s, tc) }()

	// Many cheap trials in which cancellation, request enqueue and message
	// availability are deliberately interleaved. The trials make NO
	// assumption about the scheduler: whatever order wins, the same
	// invariants must hold. The reservation barrier (other tests in this
	// file) provides deterministic coverage of the exact arbitration point,
	// so here it is left disarmed.
	const trials = 300
	cancelTrials := 0
	handoffTrials := 0
trialLoop:
	for i := 0; i < trials; i++ {
		ctxN := newCancelableCtx()
		rqN := newCtxRequest(ctxN)
		var relay *partitionRelay
		var rs consumer.Response

		switch i % 3 {
		case 0:
			// Message already deliverable while the request races with
			// cancellation (both wait-select cases may be ready).
			relay = spawnPartitionRelay(tc, i)
			s.requestsCh <- rqN
			ctxN.cancel()
		case 1:
			// Request is queued and canceled before any message exists, the
			// record shows up later.
			s.requestsCh <- rqN
			ctxN.cancel()
			relay = spawnPartitionRelay(tc, i)
		default:
			// Give the handoff a fair chance to win before cancellation.
			relay = spawnPartitionRelay(tc, i)
			s.requestsCh <- rqN
			deadline := time.Now().Add(200 * time.Microsecond)
			for time.Now().Before(deadline) {
				select {
				case rs := <-rqN.ResponseCh:
					// Handoff won before the client gave up; the late
					// cancellation must not undo it.
					c.Assert(rs.Err, IsNil)
					c.Assert(rs.Msg, DeepEquals, relay.msg)
					ctxN.cancel()
					handoffTrials++
					gotOffers, gotResets := drainRelay(c, relay)
					c.Assert(gotOffers, Equals, 1)
					c.Assert(gotResets, Equals, 0)
					relay.wg.Wait()
					assertNoRqResponse(c, rqN)
					continue trialLoop
				default:
					runtime.Gosched()
				}
			}
			ctxN.cancel()
		}

		select {
		case rs = <-rqN.ResponseCh:
		case <-time.After(2 * time.Second):
			c.Fatalf("trial %d: request not finalized", i)
		}
		assertNoRqResponse(c, rqN)

		switch rs.Err {
		case consumer.ErrRequestCanceled:
			// Whether the request died before queuing, while waiting, after
			// reserving the record or at the arbitration point, the record
			// must be offered exactly once to the next live request,
			// immediately.
			cancelTrials++
			liveRq := newCtxRequest(context.Background())
			s.requestsCh <- liveRq
			assertRqResponse(c, liveRq, consumer.Response{Msg: relay.msg}, 2*time.Second)
		case nil:
			handoffTrials++
			c.Assert(rs.Msg.Offset, Equals, int64(1000+i))
		default:
			c.Fatalf("trial %d: unexpected response: %v", i, rs)
		}

		gotOffers, gotResets := drainRelay(c, relay)
		c.Assert(gotOffers, Equals, 1)
		c.Assert(gotResets <= 1, Equals, true, Commentf("trial %d", i))
		relay.wg.Wait()
	}
	c.Logf("cancel trials: %d, handoff trials: %d", cancelTrials, handoffTrials)
	c.Assert(cancelTrials+handoffTrials, Equals, trials)
	c.Assert(cancelTrials > 0, Equals, true)
	c.Assert(handoffTrials > 0, Equals, true)
}

// An expired caller deadline ends the request with ErrRequestDeadline and
// subsequent live requests consume normally.
func (s *TopicCsmSuite) TestDeadlineWhileQueued(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = time.Hour
	tc := spawnTestTc(s, c)
	defer func() { stopTestTc(c, s, tc) }()

	ctx := newCancelableCtx().withDeadline(30 * time.Millisecond)
	rq := newCtxRequest(ctx)
	s.requestsCh <- rq

	// When: the caller deadline expires (no message available).
	time.AfterFunc(30*time.Millisecond, ctx.deadlineExceeded)

	// Then
	assertRqResponse(c, rq, consumer.Response{Err: consumer.ErrRequestDeadline}, time.Second)

	// And: a following live request consumes normally.
	relay := spawnPartitionRelay(tc, 1)
	liveRq := newCtxRequest(context.Background())
	s.requestsCh <- liveRq
	assertRqResponse(c, liveRq, consumer.Response{Msg: relay.msg}, time.Second)
	relay.assertObserved(c, outcomeOffered)
	relay.wg.Wait()
}

// Many canceled requests in a row, including ones interleaved with a reset
// record waiting for delivery, do not block or poison the next live
// consumption.
func (s *TopicCsmSuite) TestMassCancelThenConsume(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = time.Hour
	tc := spawnTestTc(s, c)
	defer func() { stopTestTc(c, s, tc) }()

	// First reserve a record for a canceled request via the barrier.
	barrierCh := make(chan *ReserveBarrier)
	AfterReserveCh = barrierCh

	ctx1 := newCancelableCtx()
	rq1 := newCtxRequest(ctx1)
	s.requestsCh <- rq1
	relay := spawnPartitionRelay(tc, 11)
	barrier := <-barrierCh
	c.Assert(barrier.Msg, DeepEquals, relay.msg)
	ctx1.cancel()
	barrier.Release()
	assertRqResponse(c, rq1, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)
	relay.assertObserved(c, outcomeReset)
	AfterReserveCh = nil

	// Then storm the topic consumer with already-canceled requests; the reset
	// record must survive all of them.
	const n = 50
	for i := 0; i < n; i++ {
		ctx := newCancelableCtx()
		ctx.cancel()
		rq := newCtxRequest(ctx)
		s.requestsCh <- rq
		assertRqResponse(c, rq, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)
		relay.assertNoOffer(c)
	}

	// Finally a live request receives the reset record immediately.
	liveRq := newCtxRequest(context.Background())
	s.requestsCh <- liveRq
	assertRqResponse(c, liveRq, consumer.Response{Msg: relay.msg}, time.Second)
	relay.assertObserved(c, outcomeOffered)
	relay.wg.Wait()
}

// A record reserved by a canceled request is returned to its partition before
// the topic subscription expires: it was never offered, therefore the
// winding-down topic consumer is safe to stop and the record stays
// redeliverable from the last committed offset.
func (s *TopicCsmSuite) TestResetRecordExpiresSafely(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 300
	tc := spawnTestTc(s, c)

	barrierCh := make(chan *ReserveBarrier)
	AfterReserveCh = barrierCh
	defer func() { AfterReserveCh = nil }()

	ctx1 := newCancelableCtx()
	rq1 := newCtxRequest(ctx1)
	s.requestsCh <- rq1
	relay := spawnPartitionRelay(tc, 5)
	barrier := <-barrierCh
	ctx1.cancel()
	barrier.Release()
	assertRqResponse(c, rq1, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)
	relay.assertObserved(c, outcomeReset)
	AfterReserveCh = nil

	// When: the subscription expires and it is safe to stop.
	c.Assert(clock.Advance(500), Equals, time.Duration(500))

	// Then: the topic consumer stops despite the record being pending
	// redelivery, and it was never offered.
	assertStopped(c, s.lifespanCh, time.Second*5)
	relay.assertNoOffer(c)
	relay.drainAfterStop(tc)
}

// A canceled request arriving during the safe-stop wait must not revive the
// topic consumer.
func (s *TopicCsmSuite) TestCanceledDuringSafeStop(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 700
	s.setSafe2Stop(false)
	safe2StopPollingInterval = 5
	spawnTestTc(s, c)

	// Subscription expires, winding down starts.
	c.Assert(clock.Advance(500), Equals, time.Duration(500))
	assertRunning(c, s.lifespanCh, 100*time.Millisecond)

	// When: a canceled request arrives while waiting for a safe stop.
	ctx := newCancelableCtx()
	ctx.cancel()
	rq := newCtxRequest(ctx)
	s.requestsCh <- rq
	assertRqResponse(c, rq, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)

	// Then: still winding down (no revival); eventually stops unsafely at
	// AckTimeout just as it would without the canceled request.
	c.Assert(clock.Advance(190), Equals, time.Duration(690))
	assertRunning(c, s.lifespanCh, 300*time.Millisecond)
	c.Assert(clock.Advance(14), Equals, time.Duration(704))
	assertStopped(c, s.lifespanCh, time.Second*5)
}

// Normal stop while a record is pending redelivery after a canceled
// reservation terminates the topic consumer right away; the canceled waiting
// request is never finalized twice.
func (s *TopicCsmSuite) TestStopWithResetRecord(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = time.Hour
	tc := spawnTestTc(s, c)

	barrierCh := make(chan *ReserveBarrier)
	AfterReserveCh = barrierCh
	defer func() { AfterReserveCh = nil }()

	ctx1 := newCancelableCtx()
	rq1 := newCtxRequest(ctx1)
	s.requestsCh <- rq1
	relay := spawnPartitionRelay(tc, 9)
	barrier := <-barrierCh
	ctx1.cancel()
	barrier.Release()
	assertRqResponse(c, rq1, consumer.Response{Err: consumer.ErrRequestCanceled}, time.Second)
	relay.assertObserved(c, outcomeReset)
	AfterReserveCh = nil

	// When: dispatcher signals shutdown.
	close(s.requestsCh)

	// Then
	assertStopped(c, s.lifespanCh, time.Second*5)
	assertNoRqResponse(c, rq1)
	relay.assertNoOffer(c)
	relay.drainAfterStop(tc)
}
