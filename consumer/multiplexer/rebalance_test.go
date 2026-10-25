package multiplexer

import (
	"sync/atomic"
	"time"

	. "gopkg.in/check.v1"

	"github.com/mailgun/kafka-pixy/consumer"
)

// recordingMockIn wraps mockIn to count how many times Stop was invoked, so
// tests can assert that partition handoff does not finalize an input twice.
type recordingMockIn struct {
	*mockIn
	stopCount int32
}

func newRecordingMockIn(safe2Stop bool, messages ...consumer.Message) *recordingMockIn {
	mi := newMockIn(messages...)
	mi.safe2Stop = safe2Stop
	return &recordingMockIn{mockIn: mi}
}

func (r *recordingMockIn) Stop() {
	atomic.AddInt32(&r.stopCount, 1)
}

func (r *recordingMockIn) stopCalls() int32 {
	return atomic.LoadInt32(&r.stopCount)
}

// Rebalance while the multiplexer is blocked sending a message to an output
// that canceled the in-flight request: the interrupted send stores the
// message, WireUp returns promptly (no leaked waiter) and the message is
// redelivered exactly once to the next wired, reading output. The retained
// input is stopped exactly once during final shutdown.
func (s *MultiplexerSuite) TestRebalanceWhileBlockedSend(c *C) {
	in1 := newRecordingMockIn(true, msg(1001, 1))
	ins := map[int32]In{1: in1}
	m := New(s.ns, func(p int32) In { return ins[p] })

	// An unbuffered output that never reads (simulates a topic consumer that
	// canceled before rendezvous).
	gateOut := newMockOut(0)
	m.WireUp(gateOut, []int32{1})
	time.Sleep(50 * time.Millisecond) // let mux block in the output send

	// When: rewired to a reading output while the send is blocked.
	readingOut := newMockOut(10)
	m.WireUp(readingOut, []int32{1})

	// Then: the stored message is redelivered exactly once.
	checkMsg(c, readingOut.messagesCh, msg(1001, 1))
	select {
	case m := <-readingOut.messagesCh:
		c.Errorf("Unexpected duplicate message: %v", m)
	case <-time.After(100 * time.Millisecond):
	}
	c.Assert(m.IsRunning(), Equals, true)

	// Final shutdown stops the retained input exactly once.
	m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	c.Assert(in1.stopCalls(), Equals, int32(1))
}

// A message already reserved by the output (rendezvous completed, as in a
// post-reservation cancel in the topic consumer) must not block a subsequent
// partition handoff: the removed input is stopped exactly once, WireUp
// returns promptly, and a new owner of the partition receives the offset
// again (the old partition consumer never marked it offered).
func (s *MultiplexerSuite) TestRebalanceAfterReservation(c *C) {
	in1 := newRecordingMockIn(true, msg(1001, 1))
	ins := map[int32]In{1: in1}
	m := New(s.ns, func(p int32) In { return ins[p] })

	// Unbuffered output with a relay that receives exactly one message and
	// then holds it (simulates reservation + canceled request).
	holdOut := newMockOut(0)
	relayedCh := make(chan consumer.Message, 1)
	go func() {
		relayedCh <- (<-holdOut.messagesCh)
	}()

	m.WireUp(holdOut, []int32{1})
	c.Assert(<-relayedCh, DeepEquals, msg(1001, 1))

	// When: the partition is reassigned away while the output holds the
	// reserved record.
	doneCh := make(chan struct{})
	go func() {
		m.WireUp(holdOut, []int32{})
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		c.Fatal("WireUp leaked a waiter while the output held a reserved message")
	}
	c.Assert(m.IsRunning(), Equals, false)
	c.Assert(in1.stopCalls(), Equals, int32(1))
	select {
	case m := <-holdOut.messagesCh:
		c.Errorf("Unexpected duplicate delivery to holding output: %v", m)
	default:
	}

	// Then: a new owner (new partition consumer input) replays the offset
	// exactly once.
	in2 := newRecordingMockIn(true, msg(1001, 1))
	ins[1] = in2
	newOut := newMockOut(10)
	m.WireUp(newOut, []int32{1})
	checkMsg(c, newOut.messagesCh, msg(1001, 1))
	select {
	case m := <-newOut.messagesCh:
		c.Errorf("Unexpected duplicate message to new owner: %v", m)
	case <-time.After(100 * time.Millisecond):
	}

	m.Stop()
	c.Assert(in1.stopCalls(), Equals, int32(1)) // old input not finalized twice
	c.Assert(in2.stopCalls(), Equals, int32(1)) // new input finalized once
}
