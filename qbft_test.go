package qbft_test

import (
	"context"
	"fmt"
	"github.com/corverroos/qbft"
	"math/rand"
	"testing"
	"time"
)

func TestHappy(t *testing.T) {
	const (
		n = 4
		q = 3
		f = 1

		instance = 1 // Vary this see the results change
	)

	defs := qbft.Defs{
		IsLeader: func(instance, round int64, process int64) bool {
			return (instance+round)%n == process
		},
		NewTimer: func(round int64) (<-chan time.Time, func()) {
			timer := time.NewTimer(time.Minute)
			return timer.C, func() { timer.Stop() }
		},
		IsValid: func(instance int64, msg qbft.Msg) bool {
			return true
		},
		Quorum: q,
		Faulty: f,
	}

	var receives []chan qbft.Msg
	broadcast := make(chan qbft.Msg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := make(chan string, n)

	for i := int64(0); i < n; i++ {
		receive := make(chan qbft.Msg, 1000)
		receives = append(receives, receive)
		trans := qbft.Transport{
			Broadcast: func(msg qbft.Msg) {
				broadcast <- msg
			},
			Receive: receive,
		}

		go func(i int64) {
			result, err := qbft.Run(ctx, defs, trans, instance, i, []byte(fmt.Sprint(i)))
			if err != nil {
				t.Fatal(err)
				return
			}
			resultChan <- string(result)
		}(i)
	}

	var results []string

	for {
		select {
		case msg := <-broadcast:
			for _, out := range receives {
				out <- msg
				if rand.Float64() < 0.1 { // Send 10% messages twice
					out <- msg
				}
			}
		case result := <-resultChan:
			results = append(results, result)
			if len(results) == n {
				t.Logf("Got all results: %v", results)
				return
			}
		}
	}
}
