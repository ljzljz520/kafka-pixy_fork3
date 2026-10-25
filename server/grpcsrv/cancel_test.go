package grpcsrv

import (
	"testing"

	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/proxy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapConsumeError(t *testing.T) {
	cases := []struct {
		err  error
		code codes.Code
	}{
		{consumer.ErrRequestCanceled, codes.Canceled},
		{consumer.ErrRequestDeadline, codes.DeadlineExceeded},
		{consumer.ErrRequestTimeout, codes.NotFound},
		{consumer.ErrTooManyRequests, codes.ResourceExhausted},
		{consumer.ErrUnavailable, codes.Unavailable},
		{proxy.ErrDisabled, codes.Unavailable},
		{proxy.ErrUnavailable, codes.Unavailable},
		{errSomeUnexpected, codes.Internal},
	}
	for i, tc := range cases {
		got := status.Code(mapConsumeError(tc.err))
		if got != tc.code {
			t.Errorf("case %d: err=%v want=%s got=%s", i, tc.err, tc.code, got)
		}
	}
}

var errSomeUnexpected = errSentinel("boom")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
