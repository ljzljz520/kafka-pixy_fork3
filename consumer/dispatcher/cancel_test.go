package dispatcher

import (
	"context"
	"time"

	"github.com/mailgun/kafka-pixy/consumer"
	. "gopkg.in/check.v1"
)

// A request whose context is already canceled is dropped at the dispatcher
// boundary: no group/topic child is created for it and its response channel is
// finalized exactly once with ErrRequestCanceled.
func (s *DispatcherSuite) TestCanceledDroppedBeforeChild(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rq := consumer.NewRequestCtx(ctx, "g1", "t1")

	// When
	d.Requests() <- rq

	// Then
	assertRejected(c, rq, consumer.ErrRequestCanceled, time.Second)
	assertNoChildSpawned(c, s.groupF, 100*time.Millisecond)
}

// Same as above but for an already expired deadline: the response must report
// ErrRequestDeadline and still no child is created.
func (s *DispatcherSuite) TestDeadlineDroppedBeforeChild(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond)
	rq := consumer.NewRequestCtx(ctx, "g1", "t1")

	// When
	d.Requests() <- rq

	// Then
	assertRejected(c, rq, consumer.ErrRequestDeadline, time.Second)
	assertNoChildSpawned(c, s.groupF, 100*time.Millisecond)
}

// A canceled request buffered in a child's queue is observable as canceled by
// the downstream tier, and subsequent live requests keep flowing to the same
// child: a canceled request neither corrupts the queue nor takes the slot of
// the following request.
func (s *DispatcherSuite) TestCanceledWhileQueuedInChild(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	rq1 := consumer.NewRequestCtx(ctx, "g1", "t1")
	d.Requests() <- rq1
	child := <-s.groupF.spawnedCh
	defer child.Dispose()

	// rq1 sits in the child queue while the downstream tier is busy; the
	// client gives up.
	c.Assert(<-child.Requests(), Equals, rq1)
	cancel()
	c.Assert(rq1.Context().Err(), Equals, context.Canceled)

	// Downstream finalizes the queued request exactly once, the same way the
	// topic consumer does for a canceled request.
	rq1.ResponseCh <- consumer.CanceledResponse(rq1.Context().Err())
	assertRejected(c, rq1, consumer.ErrRequestCanceled, time.Second)

	// Then: the next live request is still dispatched to the same child and
	// carries a live context.
	rq2 := consumer.NewRequest("g1", "t2")
	d.Requests() <- rq2
	c.Assert(<-child.Requests(), Equals, rq2)
	c.Assert(rq2.Context().Err(), IsNil)
}

// Many consecutive canceled requests must not spawn anything and must not
// block or poison subsequent live requests.
func (s *DispatcherSuite) TestMassCancelThenLiveConsume(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	const n = 50
	rqs := make([]consumer.Request, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rqs[i] = consumer.NewRequestCtx(ctx, "g1", "t1")
		d.Requests() <- rqs[i]
	}
	for i := 0; i < n; i++ {
		assertRejected(c, rqs[i], consumer.ErrRequestCanceled, time.Second)
	}
	assertNoChildSpawned(c, s.groupF, 100*time.Millisecond)

	// Then: a live request after the storm spawns a child and flows.
	liveRq := consumer.NewRequest("g1", "t1")
	d.Requests() <- liveRq
	child := <-s.groupF.spawnedCh
	defer child.Dispose()
	c.Assert(<-child.Requests(), Equals, liveRq)
}

// When the child buffer is full, a request whose context is already done is
// reported as canceled rather than as an overflow rejection, while a live
// request still gets the overflow rejection.
func (s *DispatcherSuite) TestCancelAtFullBuffer(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)

	// Fill the g1 child buffer up to its capacity with undrained requests.
	stuck := make([]consumer.Request, s.cfg.Consumer.ChannelBufferSize)
	for i := range stuck {
		stuck[i] = consumer.NewRequest("g1", "t1")
		d.Requests() <- stuck[i]
	}
	child := <-s.groupF.spawnedCh

	// A canceled request at full capacity is classified as canceled, not as
	// an overflow.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceledRq := consumer.NewRequestCtx(ctx, "g1", "t1")
	d.Requests() <- canceledRq
	assertRejected(c, canceledRq, consumer.ErrRequestCanceled, time.Second)

	// A live one at full capacity still gets the ordinary overflow error.
	overflowRq := consumer.NewRequest("g1", "t1")
	d.Requests() <- overflowRq
	assertRejected(c, overflowRq, consumer.ErrTooManyRequests, time.Second)

	// The queued requests must not have received any response.
	for _, rq := range stuck {
		assertNoResponse(c, rq)
	}

	// Drain the child queue (acting as the downstream tier) so that disposing
	// it does not spawn an unmanaged successor, then stop the dispatcher.
	for range stuck {
		<-child.Requests()
	}
	child.Dispose()
	d.Stop()
}

func assertNoChildSpawned(c *C, ff *fakeFactory, timeout time.Duration) {
	select {
	case cs := <-ff.spawnedCh:
		c.Errorf("Unexpected child spawned: %v", cs)
	case <-time.After(timeout):
	}
}
