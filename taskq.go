// Package taskq implements an intra-process task queue with a simple
// retry model.
package taskq

import (
	"errors"
	"log"
	"sync"
	"time"
)

// Task is the execution unit.
type Task interface {
	// Do executes the task. Errors returned are logged to the default
	// logger.
	Do() error
}

// Func represents a function that is a Task.
type Func func() error

// Do executes the task function.
func (t Func) Do() error {
	return t()
}

// Queue manages a fixed-sized queue of tasks to be executed at a fixed
// rate by a number of parallel workers.
type Queue struct {
	taskc         chan Task
	waitg         sync.WaitGroup
	tickermu      sync.Mutex
	ticker        *time.Ticker
	retries       int
	retrydelay    time.Duration
	retrymaxdelay time.Duration
}

var errWorkersLessThanOne = errors.New("taskq: workers less than one")

// New creates a new queue with the specified queue size. Panics if size is
// less than zero.
func New(size int) *Queue {
	q := &Queue{
		taskc:         make(chan Task, size),
		ticker:        time.NewTicker(time.Second),
		retrydelay:    1 * time.Second,
		retrymaxdelay: 5 * time.Minute,
	}
	return q
}

// Start spawns n worker goroutines to execute tasks. Panics if n is
// less than 1.
func (q *Queue) Start(n int) {
	if n < 1 {
		panic(errWorkersLessThanOne)
	}
	q.waitg.Add(n)
	for i := 0; i < n; i++ {
		go q.worker()
	}
}

// Add adds a task to the queue, blocking if the queue is full.
func (q *Queue) Add(t Task) {
	q.taskc <- t
}

// Stop closes the queue and waits for the remaining tasks to be
// executed.
func (q *Queue) Stop() {
	close(q.taskc)
	q.waitg.Wait()
	q.ticker.Stop()
}

// SetInterval sets minimum time interval between tasks. Panics if not
// greater than zero. Default is 1 * time.Second.
func (q *Queue) SetInterval(dur time.Duration) {
	q.tickermu.Lock()
	defer q.tickermu.Unlock()
	q.ticker.Stop()
	q.ticker = time.NewTicker(dur)
}

// SetRetries sets maximum number of retries before dropping a task.  If
// retries is zero, never drop the task. Default is zero.
func (q *Queue) SetRetries(retries int) {
	q.retries = retries
}

// SetRetryDelay sets the first retry delay duration, and maximum delay
// after doubling the delay for each retry. Default delay of
// 1 * time.Second, max of 5 * time.Minute.
func (q *Queue) SetRetryDelay(delay, maxdelay time.Duration) {
	q.retrydelay = delay
	q.retrymaxdelay = maxdelay
}

// throttle waits for ticker or new interval duration
func (q *Queue) throttle() {
	q.tickermu.Lock()
	<-q.ticker.C
	q.tickermu.Unlock()
}

type retryState struct {
	n        int
	max      int
	delay    time.Duration
	maxdelay time.Duration
}

// backoff sleeps then returns true if task should be retried
func (r *retryState) backoff() bool {
	r.n++
	time.Sleep(r.delay)
	if r.delay < r.maxdelay {
		r.delay *= 2
		if r.delay > r.maxdelay {
			r.delay = r.maxdelay
		}
	}
	return r.max > 0 && r.n < r.max
}

func (r *retryState) reset(q *Queue) {
	r.n = 0
	r.max = q.retries
	r.delay = q.retrydelay
	r.maxdelay = q.retrymaxdelay
}

// worker pulls tasks from the queue and executes them until the queue
// is stopped.
func (q *Queue) worker() {
	defer q.waitg.Done()
	var retry retryState
	retry.reset(q)
	for t := range q.taskc {
		q.throttle()
		for err := t.Do(); err != nil; err = t.Do() {
			log.Println(err)
			if !retry.backoff() {
				break
			}
		}
		retry.reset(q)
	}
}
