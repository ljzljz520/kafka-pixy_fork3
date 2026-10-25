package consumerimpl

import (
	"context"
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/testhelpers"
)

// TestCanceledDroppedBeforeSpawn drives the real two-tier dispatcher chain of
// a consumerimpl instance without any Kafka/Zookeeper connection: requests
// whose context is already terminal must be dropped by the group dispatcher
// before any group child (groupcsm) is spawned, therefore the nil Kafka
// clients in this test are never touched. Run it standalone with
// `go test -run TestCanceledDroppedBeforeSpawn` (the gocheck suite in this
// package requires a live Kafka cluster).
func TestCanceledDroppedBeforeSpawn(tt *testing.T) {
	cfg := testhelpers.NewTestProxyCfg("cancel_no_kafka")
	cons := &t{
		actDesc: actor.Root().NewChild("cons_cancel"),
		cfg:     cfg,
	}
	cons.dispatcher = dispatcher.Spawn(cons.actDesc, cons, cfg)
	defer cons.dispatcher.Stop()

	const n = 30
	rchs := make([]<-chan consumer.Response, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rchs[i] = cons.AsyncConsumeCtx(ctx, "g1", "t1")
	}
	for i, ch := range rchs {
		select {
		case rs := <-ch:
			if rs.Err != consumer.ErrRequestCanceled {
				tt.Fatalf("rq %d: want=%v got=%v", i, consumer.ErrRequestCanceled, rs.Err)
			}
		case <-time.After(2 * time.Second):
			tt.Fatalf("rq %d: canceled request was not finalized", i)
		}
	}

	// Expired deadlines get the deadline error, also without spawning.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond)
	ch := cons.AsyncConsumeCtx(ctx, "g1", "t1")
	select {
	case rs := <-ch:
		if rs.Err != consumer.ErrRequestDeadline {
			tt.Fatalf("want=%v got=%v", consumer.ErrRequestDeadline, rs.Err)
		}
	case <-time.After(2 * time.Second):
		tt.Fatal("deadline request was not finalized")
	}
}
