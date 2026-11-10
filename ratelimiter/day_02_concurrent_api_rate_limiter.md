# Day 2 — Concurrent API Rate Limiter (Go)

**Difficulty:** Medium–Hard  
**Language:** Go  
**Suggested time:** 60–90 minutes  
**Topic:** Low-level design, concurrency, rate limiting

## Problem statement

You are building an API gateway serving thousands of clients. Each client is identified by a unique API key. Implement a **thread-safe, per-client rate limiter** using the **Token Bucket** algorithm.

Every incoming request must be allowed or rejected based on the tokens available to that client's bucket.

## Token Bucket rules

- Each client has an independent bucket.
- A new bucket starts with its full capacity of tokens.
- An allowed request consumes one token (or `N` tokens for `AllowN`).
- Tokens replenish continuously at the configured refill rate, measured in tokens per second.
- Tokens never exceed the configured capacity.
- If insufficient tokens exist, `Allow` and `AllowN` return `false` immediately without consuming tokens.
- Fractional tokens may accumulate internally, even though requests consume whole tokens.
- Refill must be calculated correctly after long periods of inactivity; no background refill goroutine per client is allowed.

## Functional requirements

1. Support separate rate limits for each `clientID`.
2. Configure bucket capacity, token refill rate, and an idle-expiration duration.
3. Implement `Allow(clientID)` to consume one token if available.
4. Implement `AllowN(clientID, tokens)` to atomically consume multiple tokens if available.
5. Implement `Wait(ctx, clientID)` to wait for one token, respecting context cancellation and avoiding busy waiting.
6. Support simultaneous requests from many goroutines without data races or unintended overspending of tokens.
7. Remove idle client buckets via `Cleanup()` to bound memory usage.
8. Implement `Close()` so future calls cannot acquire tokens and waiting calls are unblocked.

## Constraints

- Use only Go's standard library.
- Do not use Redis, databases, or third-party rate-limit libraries.
- Do not start one goroutine per client for token replenishment.
- Avoid busy waiting.
- Keep synchronization efficient so unrelated clients do not unnecessarily block each other.
- Define and document handling for invalid configuration, invalid token counts, and requests larger than bucket capacity.

## Interface to implement

```go
package ratelimiter

import (
    "context"
    "time"
)

type Config struct {
    Capacity   int
    RefillRate float64 // Tokens per second
    IdleTTL    time.Duration
}

type RateLimiter interface {
    Allow(clientID string) bool
    AllowN(clientID string, tokens int) bool
    Wait(ctx context.Context, clientID string) error
    Cleanup()
    Close()
}

func NewRateLimiter(cfg Config) (RateLimiter, error) {
    panic("implement me")
}
```

You may introduce additional private structs, methods, and error values. Specify how `Wait` reports a closed limiter.

## Example scenario

Given:

```go
Config{
    Capacity:   5,
    RefillRate: 2,
    IdleTTL:    5 * time.Minute,
}
```

Assume a new client's bucket starts full, all events occur in the order shown, and no other requests occur:

| Elapsed time | Action | Available tokens before request(s) | Expected behavior |
|---|---|---|---|
| 0.0s | 5 requests | 5 | All 5 allowed; balance becomes 0 |
| 0.1s | 1 request | 0.2 | Rejected; balance stays 0.2 |
| 0.5s | 1 request | 1.0 | Allowed; balance becomes 0 |
| 2.5s | 5 requests | 4.0 | 4 allowed, 1 rejected |

The example uses idealized timings; real tests should account for scheduler and clock tolerances.

## Concurrency and lifecycle edge cases to consider

- 100 goroutines call `Allow` on the same client at exactly the same time.
- Many goroutines call `Allow` on different clients simultaneously.
- A client has been idle for hours before making another request.
- `AllowN` requests more tokens than the bucket could ever hold.
- `Wait` is blocked when its context is cancelled.
- `Wait` is blocked when `Close` is called.
- `Cleanup` runs concurrently with requests and waiting callers.
- `Close` is called multiple times or concurrently with `Cleanup`.

Decide and document the cleanup semantics carefully: what should happen if an idle bucket is removed while a request is waiting on that client's tokens?

## Expected deliverables

1. Working Go implementation of the interface.
2. Unit tests for refill behavior, burst capacity, and client isolation.
3. Concurrency tests for oversubscription and lifecycle races.
4. Tests for cancellation, cleanup, invalid inputs, and close behavior.
5. A brief explanation of synchronization choices and trade-offs.

Run your tests with:

```bash
go test ./...
go test -race ./...
```

## Evaluation rubric (100 points)

| Area | Weight |
|---|---:|
| Token bucket correctness | 25 |
| Concurrency safety | 25 |
| API design and error handling | 15 |
| Context cancellation and lifecycle | 15 |
| Memory cleanup | 10 |
| Tests, including race detection | 10 |

## Starting point — design before code

Sketch the internal data structures for:

- **Bucket:** token balance, capacity, most recent refill time, and synchronization.
- **Limiter:** client-bucket registry and lifecycle management.
- **Config:** validated shared settings.

Then implement `Allow()` before tackling `Wait()`, `Cleanup()`, and `Close()`.

**Design prompt:** How can you protect the shared client map without serializing every request to unrelated clients?

---

**Challenge rule:** Implement the solution yourself first. Explain the invariants your locks protect, then test under `go test -race`.
