package broker

import "testing"

func TestRateLimitKeyFormat(t *testing.T) {
	if got := ratelimitKey("emails"); got != "q:emails:ratelimit" {
		t.Errorf("ratelimitKey = %q, want %q", got, "q:emails:ratelimit")
	}
}

func TestWithRateLimitRegisters(t *testing.T) {
	b := New(nil, WithRateLimit("emails", 100, 200))
	rl, ok := b.rateLimits["emails"]
	if !ok || rl.rate != 100 || rl.burst != 200 {
		t.Errorf("rateLimits[emails] = %+v ok=%v, want {rate:100 burst:200}", rl, ok)
	}
	if _, ok := b.rateLimits["sms"]; ok {
		t.Error("sms should have no limit registered")
	}
}

func TestWithRateLimitPanicsOnBadConfig(t *testing.T) {
	cases := []struct {
		rate  float64
		burst int
	}{{0, 10}, {-1, 10}, {100, 0}, {100, -1}}
	for _, c := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithRateLimit(rate=%v, burst=%d) did not panic", c.rate, c.burst)
				}
			}()
			New(nil, WithRateLimit("q", c.rate, c.burst))
		}()
	}
}
