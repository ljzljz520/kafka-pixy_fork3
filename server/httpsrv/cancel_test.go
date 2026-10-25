package httpsrv

import (
	"net/http"
	"testing"

	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/proxy"
)

func TestConsumeHTTPStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{consumer.ErrRequestCanceled, statusClientClosedRequest},
		{consumer.ErrRequestDeadline, http.StatusGatewayTimeout},
		{consumer.ErrRequestTimeout, http.StatusRequestTimeout},
		{consumer.ErrTooManyRequests, http.StatusTooManyRequests},
		{consumer.ErrUnavailable, http.StatusServiceUnavailable},
		{proxy.ErrDisabled, http.StatusServiceUnavailable},
		{proxy.ErrUnavailable, http.StatusServiceUnavailable},
		{errSentinel("boom"), http.StatusInternalServerError},
	}
	for i, tc := range cases {
		if got := consumeHTTPStatus(tc.err); got != tc.want {
			t.Errorf("case %d: err=%v want=%d got=%d", i, tc.err, tc.want, got)
		}
	}
}

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
