package concurrentpaymentledger

import (
	"context"
	"errors"
	"sync"
)

var ErrPoolClosed = errors.New("worker pool is closed")

type Job[T any] struct {
	ID      string
	Index   int
	Execute func(context.Context) (T, error)
}

type Result[T any] struct {
	ID    string
	Index int
	Value T
	Err   error
}

type Pool[T any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	jobs    chan Job[T]
	results chan Result[T]

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func New[T any](workers, queueSize int) *Pool[T] {
	if workers <= 0 || queueSize < 0 {
		panic("Invalid pool config")
	}
	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool[T]{
		jobs:    make(chan Job[T], queueSize),
		results: make(chan Result[T], workers),
		ctx:     ctx,
		cancel:  cancel,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for {
		if p.ctx.Err() != nil {
			return
		}
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			if p.ctx.Err() != nil {
				return
			}
			value, err := job.Execute(p.ctx)
			select {
			case p.results <- Result[T]{
				ID:    job.ID,
				Index: job.Index,
				Value: value,
				Err:   err,
			}:
			case <-p.ctx.Done():
				return
			}

		}
	}
}

func (p *Pool[T]) Submit(ctx context.Context, job Job[T]) error {
	// if job == nil {
	// 	return errors.New("job is nil")
	// }
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return ErrPoolClosed
	case p.jobs <- job:
		return nil
	}
}

func (p *Pool[T]) Shutdown() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()
	p.wg.Wait()
}
func (p *Pool[T]) Cancel() {
	p.mu.Lock()

	if !p.closed {
		p.closed = true
		close(p.jobs)
	}

	p.cancel()
	p.mu.Unlock()

	// Running jobs must respect ctx cancellation.
	p.wg.Wait()
}
