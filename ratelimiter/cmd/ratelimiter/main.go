package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	limiter "ratelimiter"
)

func expect(got, want bool, label string) error {
	if got != want {
		return fmt.Errorf("%s: got %v, want %v", label, got, want)
	}
	return nil
}

func concurrentRequests(l *limiter.InMemoryLimter, users, requests int) []int64 {
	counts := make([]int64, users)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for user := 0; user < users; user++ {
		for request := 0; request < requests; request++ {
			wg.Add(1)
			go func(user int) {
				defer wg.Done()
				<-start
				if l.Allow(fmt.Sprintf("user-%d", user)) {
					atomic.AddInt64(&counts[user], 1)
				}
			}(user)
		}
	}
	close(start)
	wg.Wait()
	return counts
}

func main() {
	tests := []struct {
		name string
		run  func() error
	}{
		{"Full initial bucket and burst limit", func() error {
			l := limiter.New()
			for i := 0; i < limiter.MAX_TOKEN; i++ {
				if err := expect(l.Allow("A"), true, fmt.Sprintf("request %d", i+1)); err != nil {
					return err
				}
			}
			return expect(l.Allow("A"), false, "request beyond capacity")
		}},
		{"Independent client buckets", func() error {
			l := limiter.New()
			if err := expect(l.AllowN("A", limiter.MAX_TOKEN), true, "drain A"); err != nil {
				return err
			}
			if err := expect(l.Allow("A"), false, "A exhausted"); err != nil {
				return err
			}
			return expect(l.AllowN("B", limiter.MAX_TOKEN), true, "B remains full")
		}},
		{"Invalid token counts do not consume tokens", func() error {
			l := limiter.New()
			for _, n := range []int{0, -1, limiter.MAX_TOKEN + 1} {
				if err := expect(l.AllowN("A", n), false, fmt.Sprintf("AllowN(%d)", n)); err != nil {
					return err
				}
			}
			return expect(l.AllowN("A", limiter.MAX_TOKEN), true, "full capacity after invalid requests")
		}},
		{"AllowN consumes exactly N tokens", func() error {
			l := limiter.New()
			if err := expect(l.AllowN("A", 3), true, "consume three"); err != nil {
				return err
			}
			if err := expect(l.AllowN("A", 2), true, "consume remaining two"); err != nil {
				return err
			}
			return expect(l.Allow("A"), false, "bucket exhausted")
		}},
		{"Rejected AllowN does not partially consume tokens", func() error {
			l := limiter.New()
			if err := expect(l.AllowN("A", 4), true, "consume four"); err != nil {
				return err
			}
			if err := expect(l.AllowN("A", 2), false, "reject two with only one available"); err != nil {
				return err
			}
			return expect(l.Allow("A"), true, "remaining token preserved")
		}},
		{"Fractional refill accumulates across rejected requests", func() error {
			l := limiter.New()
			if err := expect(l.AllowN("A", limiter.MAX_TOKEN), true, "drain bucket"); err != nil {
				return err
			}
			// With two tokens/second, each 300ms interval is less than one token.
			time.Sleep(300 * time.Millisecond)
			if err := expect(l.Allow("A"), false, "less than one token refilled"); err != nil {
				return err
			}
			time.Sleep(300 * time.Millisecond)
			return expect(l.Allow("A"), true, "fractional refill preserved after rejection")
		}},
		{"Refill is capped at bucket capacity", func() error {
			l := limiter.New()
			if err := expect(l.AllowN("A", limiter.MAX_TOKEN), true, "drain bucket"); err != nil {
				return err
			}
			time.Sleep(3 * time.Second)
			if err := expect(l.AllowN("A", limiter.MAX_TOKEN), true, "full bucket after idle"); err != nil {
				return err
			}
			return expect(l.Allow("A"), false, "no tokens above capacity")
		}},
		{"100 concurrent requests share one bucket", func() error {
			counts := concurrentRequests(limiter.New(), 1, 100)
			if counts[0] != int64(limiter.MAX_TOKEN) {
				return fmt.Errorf("allowed %d requests, want %d", counts[0], limiter.MAX_TOKEN)
			}
			return nil
		}},
		{"Concurrent clients each receive their own capacity", func() error {
			counts := concurrentRequests(limiter.New(), 10, 20)
			for user, count := range counts {
				if count != int64(limiter.MAX_TOKEN) {
					return fmt.Errorf("user %d: allowed %d requests, want %d", user, count, limiter.MAX_TOKEN)
				}
			}
			return nil
		}},

		{"Wait consumes one available token", func() error {
			l := limiter.New()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			allowed, err := l.Wait(ctx, "A")
			if err != nil || !allowed {
				return fmt.Errorf("Wait: allowed=%v err=%v", allowed, err)
			}
			if err := expect(l.AllowN("A", limiter.MAX_TOKEN-1), true, "remaining capacity"); err != nil {
				return err
			}
			return expect(l.Allow("A"), false, "Wait consumed one token")
		}},
		{"Wait blocks until refill and consumes the new token", func() error {
			l := limiter.New()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			if !l.AllowN("A", limiter.MAX_TOKEN) {
				return fmt.Errorf("could not drain bucket")
			}
			allowed, err := l.Wait(ctx, "A")
			elapsed := time.Since(start)
			if err != nil || !allowed {
				return fmt.Errorf("Wait: allowed=%v err=%v", allowed, err)
			}
			if elapsed < 450*time.Millisecond {
				return fmt.Errorf("returned before refill: %v", elapsed)
			}
			return expect(l.Allow("A"), false, "refilled token consumed by Wait")
		}},
		{"Already canceled Wait preserves available tokens", func() error {
			l := limiter.New()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			allowed, err := l.Wait(ctx, "A")
			if allowed || !errors.Is(err, context.Canceled) {
				return fmt.Errorf("got (%v, %v), want (false, context.Canceled)", allowed, err)
			}
			return expect(l.AllowN("A", limiter.MAX_TOKEN), true, "canceled Wait consumed nothing")
		}},
		{"Wait respects a deadline before the next refill", func() error {
			l := limiter.New()
			if !l.AllowN("A", limiter.MAX_TOKEN) {
				return fmt.Errorf("could not drain bucket")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			allowed, err := l.Wait(ctx, "A")
			if allowed || !errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("got (%v, %v), want (false, context.DeadlineExceeded)", allowed, err)
			}
			if time.Since(start) > 400*time.Millisecond {
				return fmt.Errorf("deadline response took %v", time.Since(start))
			}
			return nil
		}},
		{"Cancel a blocked Wait while another client remains usable", func() error {
			l := limiter.New()
			if !l.AllowN("A", limiter.MAX_TOKEN) {
				return fmt.Errorf("could not drain bucket")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			type outcome struct {
				allowed bool
				err     error
			}
			done := make(chan outcome, 1)
			go func() { allowed, err := l.Wait(ctx, "A"); done <- outcome{allowed, err} }()
			time.Sleep(50 * time.Millisecond)
			select {
			case got := <-done:
				return fmt.Errorf("Wait returned before cancellation: %+v", got)
			default:
			}
			otherCtx, otherCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer otherCancel()
			allowed, err := l.Wait(otherCtx, "B")
			if err != nil || !allowed {
				return fmt.Errorf("independent client Wait: allowed=%v err=%v", allowed, err)
			}
			cancel()
			select {
			case got := <-done:
				if got.allowed || !errors.Is(got.err, context.Canceled) {
					return fmt.Errorf("got (%v, %v), want (false, context.Canceled)", got.allowed, got.err)
				}
			case <-time.After(time.Second):
				return fmt.Errorf("Wait did not unblock after cancellation")
			}
			return nil
		}},
		{"Concurrent Wait calls cannot spend the same refilled token", func() error {
			l := limiter.New()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type outcome struct {
				allowed bool
				err     error
				elapsed time.Duration
			}
			done := make(chan outcome, 3)
			startGate := make(chan struct{})
			start := time.Now()
			if !l.AllowN("A", limiter.MAX_TOKEN) {
				return fmt.Errorf("could not drain bucket")
			}
			for i := 0; i < 3; i++ {
				go func() {
					<-startGate
					allowed, err := l.Wait(ctx, "A")
					done <- outcome{allowed, err, time.Since(start)}
				}()
			}
			close(startGate)
			// At two tokens/second, three successes need at least 1.5 seconds.
			// No assertion depends on which goroutine wins a token.
			successes := 0
			for i := 0; i < 3; i++ {
				select {
				case got := <-done:
					if got.err != nil || !got.allowed {
						return fmt.Errorf("waiter: allowed=%v err=%v", got.allowed, got.err)
					}
					successes++
					available := float64(limiter.REFILL_RATE) * got.elapsed.Seconds()
					if float64(successes) > available+0.05 {
						return fmt.Errorf("%d successes after %v: exceeded refill budget", successes, got.elapsed)
					}
				case <-ctx.Done():
					return fmt.Errorf("waiters did not finish: %w", ctx.Err())
				}
			}
			return nil
		}},
	}
	fmt.Println("Cases assume capacity=5 and refill=2 tokens/second.")
	fmt.Println("Timing cases use wall time; long scheduler pauses can affect results.")
	passed := 0
	for _, tc := range tests {
		fmt.Printf("\n--- %s ---\n", tc.name)
		if err := tc.run(); err != nil {
			fmt.Printf("FAIL: %v\n", err)
		} else {
			fmt.Println("PASS")
			passed++
		}
	}
	fmt.Printf("\nRESULT: %d/%d tests passed\n", passed, len(tests))
	if passed != len(tests) {
		os.Exit(1)
	}
}
