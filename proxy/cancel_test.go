package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type CancelSuite struct{}

var _ = Suite(&CancelSuite{})

func (s *CancelSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

// fakeConsumer is a scriptable consumer.T for proxy tests.
type fakeConsumer struct {
	asyncConsumeFn func(ctx context.Context, group, topic string) <-chan consumer.Response
	lastCtx        context.Context
}

func (f *fakeConsumer) Consume(group, topic string) (consumer.Message, error) {
	rs := <-f.AsyncConsume(group, topic)
	return rs.Msg, rs.Err
}

func (f *fakeConsumer) ConsumeCtx(ctx context.Context, group, topic string) (consumer.Message, error) {
	rs := <-f.AsyncConsumeCtx(ctx, group, topic)
	return rs.Msg, rs.Err
}

func (f *fakeConsumer) AsyncConsume(group, topic string) <-chan consumer.Response {
	return f.AsyncConsumeCtx(context.Background(), group, topic)
}

func (f *fakeConsumer) AsyncConsumeCtx(ctx context.Context, group, topic string) <-chan consumer.Response {
	f.lastCtx = ctx
	return f.asyncConsumeFn(ctx, group, topic)
}

func (f *fakeConsumer) Stop() {}

func newProxyWithFake(fc consumer.T) *T {
	cfg := testhelpers.NewTestProxyCfg("proxytest")
	p := &T{
		actDesc:     actor.Root().NewChild("pxy"),
		cfg:         cfg,
		consumer:    fc,
		eventsChMap: make(map[eventsChID]chan<- consumer.Event, initEventsChMapCapacity),
	}
	return p
}

func newFakeMsg(partition int32, offset int64) (consumer.Message, chan consumer.Event) {
	eventsCh := make(chan consumer.Event, 4)
	return consumer.Message{
		ConsumerMessage: sarama.ConsumerMessage{Partition: partition, Offset: offset},
		EventsCh:        eventsCh,
	}, eventsCh
}

// waitResp runs p.Consume in a goroutine and returns its outcome.
func consumeAsync(p *T, ctx context.Context, group, topic string, ack Ack) (<-chan consumer.Message, <-chan error) {
	msgCh := make(chan consumer.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		m, err := p.Consume(ctx, group, topic, ack)
		msgCh <- m
		errCh <- err
	}()
	return msgCh, errCh
}

// If the caller context is canceled before the handoff, the proxy returns
// ErrRequestCanceled, the context is propagated downstream, and no auto ack
// occurs.
func (s *CancelSuite) TestConsumeCanceledWhileWaiting(c *C) {
	fc := &fakeConsumer{
		asyncConsumeFn: func(ctx context.Context, group, topic string) <-chan consumer.Response {
			ch := make(chan consumer.Response, 1)
			go func() {
				<-ctx.Done()
				ch <- consumer.CanceledResponse(ctx.Err())
			}()
			return ch
		},
	}
	p := newProxyWithFake(fc)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	_, err := p.Consume(ctx, "g1", "t1", NoAck())
	c.Assert(err, Equals, consumer.ErrRequestCanceled)
	c.Assert(fc.lastCtx, Equals, ctx)
	c.Assert(len(p.eventsChMap), Equals, 0)
}

// An expired caller deadline maps to ErrRequestDeadline.
func (s *CancelSuite) TestConsumeDeadline(c *C) {
	fc := &fakeConsumer{
		asyncConsumeFn: func(ctx context.Context, group, topic string) <-chan consumer.Response {
			ch := make(chan consumer.Response, 1)
			go func() {
				<-ctx.Done()
				ch <- consumer.CanceledResponse(ctx.Err())
			}()
			return ch
		},
	}
	p := newProxyWithFake(fc)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := p.Consume(ctx, "g1", "t1", NoAck())
	c.Assert(err, Equals, consumer.ErrRequestDeadline)
}

// When the handoff response arrives in the same instant that the context is
// observed done, the handoff wins the proxy tie-break and the auto ack is
// still performed (preserving the existing ack rules).
func (s *CancelSuite) TestHandoffWinsRaceAutoAck(c *C) {
	msg, eventsCh := newFakeMsg(1, 10)
	gate := make(chan struct{})
	delivered := make(chan struct{})
	fc := &fakeConsumer{
		asyncConsumeFn: func(ctx context.Context, group, topic string) <-chan consumer.Response {
			ch := make(chan consumer.Response, 1)
			go func() {
				<-gate
				ch <- consumer.Response{Msg: msg}
				close(delivered)
			}()
			return ch
		},
	}
	p := newProxyWithFake(fc)

	ctx, cancel := context.WithCancel(context.Background())
	_, errCh := consumeAsync(p, ctx, "g1", "t1", AutoAck())

	// The handoff response becomes observable first; cancellation is observed
	// by the proxy select in the same instant afterwards. Either select order
	// must resolve to the handoff.
	close(gate)
	<-delivered
	cancel()

	select {
	case err := <-errCh:
		c.Assert(err, IsNil)
	case <-time.After(2 * time.Second):
		c.Fatal("Consume did not return after handoff")
	}
	// The record is registered and auto-acked.
	select {
	case e := <-eventsCh:
		c.Assert(e, DeepEquals, consumer.Ack(10))
	case <-time.After(time.Second):
		c.Error("auto ack not delivered after handoff")
	}
	c.Assert(len(p.eventsChMap), Equals, 1)
}

// If cancellation wins strictly before the handoff, no auto ack happens and
// the partition events channel is never registered.
func (s *CancelSuite) TestCancelWinsNoAckSideEffects(c *C) {
	fc := &fakeConsumer{
		asyncConsumeFn: func(ctx context.Context, group, topic string) <-chan consumer.Response {
			ch := make(chan consumer.Response, 1)
			go func() {
				<-ctx.Done()
				ch <- consumer.CanceledResponse(ctx.Err())
			}()
			return ch
		},
	}
	p := newProxyWithFake(fc)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	_, err := p.Consume(ctx, "g1", "t1", AutoAck())
	c.Assert(err, Equals, consumer.ErrRequestCanceled)
	c.Assert(len(p.eventsChMap), Equals, 0)
}

// An explicit ack of the PREVIOUS message carried by a canceled pull is
// accepted independently: canceling the pull does not revoke the ack, and
// the canceled pull does not ack the new (non-existent) message.
func (s *CancelSuite) TestExplicitAckSurvivesPullCancel(c *C) {
	msg1, eventsCh1 := newFakeMsg(1, 10)
	fc := &fakeConsumer{
		asyncConsumeFn: func(ctx context.Context, group, topic string) <-chan consumer.Response {
			ch := make(chan consumer.Response, 1)
			ch <- consumer.Response{Msg: msg1}
			return ch
		},
	}
	p := newProxyWithFake(fc)

	// First pull succeeds with NoAck: the events channel gets registered.
	got, err := p.Consume(context.Background(), "g1", "t1", NoAck())
	c.Assert(err, IsNil)
	c.Assert(got.Offset, Equals, int64(10))

	// Second pull is already canceled and carries an explicit ack of the
	// previously handed-off message.
	fc.asyncConsumeFn = func(ctx context.Context, group, topic string) <-chan consumer.Response {
		return make(chan consumer.Response, 1) // never responds: proxy must give up on ctx
	}
	ack, err := NewAck(1, 10)
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.Consume(ctx, "g1", "t1", ack)
	c.Assert(err, Equals, consumer.ErrRequestCanceled)

	// The explicit ack of the previous message is still delivered.
	select {
	case e := <-eventsCh1:
		c.Assert(e, DeepEquals, consumer.Ack(10))
	case <-time.After(2 * time.Second):
		c.Error("explicit previous ack was revoked by pull cancellation")
	}

	// And nothing else is acked on the channel.
	select {
	case e := <-eventsCh1:
		c.Errorf("Unexpected event after canceled pull: %v", e)
	default:
	}
}

// Normal autoAck flow outside of cancellation.
func (s *CancelSuite) TestAutoAckOnNormalHandoff(c *C) {
	msg, eventsCh := newFakeMsg(3, 77)
	fc := &fakeConsumer{
		asyncConsumeFn: func(ctx context.Context, group, topic string) <-chan consumer.Response {
			ch := make(chan consumer.Response, 1)
			ch <- consumer.Response{Msg: msg}
			return ch
		},
	}
	p := newProxyWithFake(fc)

	got, err := p.Consume(context.Background(), "g", "t", AutoAck())
	c.Assert(err, IsNil)
	c.Assert(got.Offset, Equals, int64(77))
	c.Assert(<-eventsCh, DeepEquals, consumer.Ack(77))
}

// A nil consumer (shutting down) still reports ErrUnavailable even when the
// context is canceled.
func (s *CancelSuite) TestUnavailableConsumer(c *C) {
	p := newProxyWithFake(nil)
	_, err := p.Consume(context.Background(), "g", "t", NoAck())
	c.Assert(err, Equals, ErrUnavailable)
}
