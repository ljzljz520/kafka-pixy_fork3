package partitioncsm

import (
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
)

// fakeFetcher is a minimal msgfetcher.T delivering scripted messages.
type fakeFetcher struct {
	messages chan consumer.Message
	stopped  chan struct{}
}

func (f *fakeFetcher) Messages() <-chan consumer.Message { return f.messages }

func (f *fakeFetcher) Stop() { close(f.stopped) }

type fetcherFactoryShim struct {
	mf         *fakeFetcher
	realOffset int64
}

func (f *fetcherFactoryShim) Spawn(parentActDesc *actor.Descriptor, topic string,
	partition int32, offset int64,
) (msgfetcher.T, int64, error) {
	return f.mf, f.realOffset, nil
}

func (f *fetcherFactoryShim) Stop() {}

type fakeOffsetMgr struct {
	submitted []offsetmgr.Offset
}

func (m *fakeOffsetMgr) SubmitOffset(offset offsetmgr.Offset) {
	m.submitted = append(m.submitted, offset)
}

func (m *fakeOffsetMgr) CommittedOffsets() <-chan offsetmgr.Offset { return nil }

func (m *fakeOffsetMgr) Stop() {}

// TestResetSignalRedrivesNeverOfferedRecord verifies the partition consumer
// half of cancellation handling without a Kafka cluster:
//
//   - a record reserved by a request that lost to cancellation is signaled on
//     its per-record ResetCh and is redelivered unchanged;
//   - the same record can be reset repeatedly if successive requests cancel
//     again after reservation;
//   - a reset record is never offered/tracked (IsSafe2Stop stays true) and no
//     offset is submitted, so GetOffsets cannot skip it;
//   - once offered, a stale queued reset signal is ignored;
//   - stopping terminates the loop promptly.
func TestResetSignalRedrivesNeverOfferedRecord(t *testing.T) {
	cfg := testhelpers.NewTestProxyCfg("pc_cancel_test")

	const offset int64 = 10
	msg := consumer.Message{}
	msg.Offset = offset
	msg.Value = []byte("v")

	mf := &fakeFetcher{
		messages: make(chan consumer.Message, 1),
		stopped:  make(chan struct{}),
	}
	mf.messages <- msg

	pc := &T{
		actDesc:         actor.Root().NewChild("test.p0"),
		cfg:             cfg,
		topic:           "t",
		partition:       0,
		messagesCh:      make(chan consumer.Message, 1),
		eventsCh:        make(chan consumer.Event, 1),
		stopCh:          make(chan none.T),
		committedOffset: offsetmgr.Offset{Val: offset},
		submittedOffset: offsetmgr.Offset{Val: offset},
		offsetTrk:       offsettrk.New(actor.Root().NewChild("trk"), offsetmgr.Offset{Val: offset}, cfg.Consumer.AckTimeout),
		offsetMgr:       &fakeOffsetMgr{},
		msgFetcherF:     &fetcherFactoryShim{mf: mf, realOffset: offset},
	}

	loopDone := make(chan bool, 1)
	go func() { loopDone <- pc.runFetchLoop() }()

	// First armed delivery.
	select {
	case got := <-pc.Messages():
		if got.Offset != offset {
			t.Fatalf("expected offset %d, got %d", offset, got.Offset)
		}
		msg = got
	case <-time.After(2 * time.Second):
		t.Fatal("first delivery never happened")
	}

	// Not offered yet: safe to stop, nothing tracked or submitted.
	if !pc.IsSafe2Stop() {
		t.Fatal("reset record must keep the partition consumer safe to stop")
	}
	if n := len(pc.offsetMgr.(*fakeOffsetMgr).submitted); n != 0 {
		t.Fatalf("no offset must be submitted before offer, got %d", n)
	}

	// Two consecutive cancellations of reservations for the same record: it
	// must be redelivered unchanged every time.
	for i := 0; i < 2; i++ {
		msg.ResetCh <- struct{}{}
		select {
		case got := <-pc.Messages():
			if got.Offset != offset {
				t.Fatalf("redelivery %d: expected offset %d, got %d", i, offset, got.Offset)
			}
			msg = got
		case <-time.After(2 * time.Second):
			t.Fatalf("redelivery %d never happened", i)
		}
		if !pc.IsSafe2Stop() {
			t.Fatalf("redelivery %d must not count as an offer", i)
		}
	}

	// Handoff finally commits: offer the record.
	pc.eventsCh <- consumer.Event{T: consumer.EvOffered, Offset: offset}
	deadline := time.After(2 * time.Second)
	for pc.IsSafe2Stop() {
		select {
		case <-deadline:
			t.Fatal("offer was not tracked")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// A stale signal queued after the offer must not cause another delivery.
	msg.ResetCh <- struct{}{}
	select {
	case got := <-pc.Messages():
		t.Fatalf("post-offer reset must not redeliver, got offset %d", got.Offset)
	case <-time.After(100 * time.Millisecond):
	}

	// No offset submission is expected without an ack.
	if n := len(pc.offsetMgr.(*fakeOffsetMgr).submitted); n != 0 {
		t.Fatalf("expected no submitted offsets before ack, got %d", n)
	}

	close(pc.stopCh)
	select {
	case again := <-loopDone:
		if again {
			t.Fatal("expected the fetch loop to stop on stopCh, not reinitialize")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch loop did not stop")
	}
	select {
	case <-mf.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("fetcher was not stopped")
	}
}
