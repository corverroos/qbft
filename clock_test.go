package qbft_test

import (
	"sync"
	"time"
)

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	chans []chan time.Time
	times []time.Time
	stop  chan struct{}
}

func (c *fakeClock) NewTimer(d time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := len(c.chans)
	ch := make(chan time.Time, 1)
	c.chans = append(c.chans, ch)
	c.times = append(c.times, c.now.Add(d))

	return ch, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.chans[i] = nil
	}
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)

	for i, ch := range c.chans {
		if ch == nil {
			continue
		}

		deadline := c.times[i]

		if deadline.After(c.now) {
			continue
		}

		ch <- deadline

		c.chans[i] = nil
	}
}
