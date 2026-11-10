package ratelimiter

import (
	"context"
	"math"
	"sync"
	"time"
)

type TokenBucket interface {
	Allow(userId string) bool
	AllowN(userId string, token int) bool
	Wait(ctx context.Context, userId string) (bool, error)
}

type Bucket struct {
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

type InMemoryLimter struct {
	userRepo map[string]*Bucket
	mu       sync.Mutex
}

func New() *InMemoryLimter {
	return &InMemoryLimter{
		userRepo: make(map[string]*Bucket),
	}
}

func (im *InMemoryLimter) Allow(userId string) bool {
	return im.AllowN(userId, 1)
}

func (im *InMemoryLimter) AllowN(userId string, token int) bool {
	if token <= 0 || token > MAX_TOKEN {
		return false
	}
	im.mu.Lock()
	bucket, exist := im.userRepo[userId]
	if !exist {
		newBucket := &Bucket{
			tokens:     float64(MAX_TOKEN),
			lastRefill: time.Now(),
		}
		im.userRepo[userId] = newBucket
		bucket = newBucket
	}
	im.mu.Unlock()
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	timeNow := time.Now()
	newToken := math.Min(
		float64(MAX_TOKEN),
		bucket.tokens+timeNow.Sub(bucket.lastRefill).Seconds()*float64(REFILL_RATE),
	)
	bucket.tokens = newToken
	bucket.lastRefill = time.Now()

	if newToken >= float64(token) {
		bucket.tokens -= float64(token)
		return true
	}
	return false

}

func (im *InMemoryLimter) Wait(
	ctx context.Context,
	userId string,
) (bool, error) {

	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if im.Allow(userId) {
			return true, nil
		}
		im.mu.Lock()
		bucket, _ := im.userRepo[userId]
		im.mu.Unlock()
		bucket.mu.Lock()
		now := time.Now()

		curToken := math.Min(
			float64(MAX_TOKEN),
			bucket.tokens+
				now.Sub(bucket.lastRefill).Seconds()*float64(REFILL_RATE),
		)
		bucket.mu.Unlock()
		waitTime := (1 - curToken) / float64(REFILL_RATE)
		timer := time.NewTimer(time.Duration(waitTime * float64(time.Second)))

		select {
		case <-timer.C:
			allowed := im.Allow(userId)
			if allowed {
				return allowed, nil
			}

		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		}

		timer.Stop()
	}
}
