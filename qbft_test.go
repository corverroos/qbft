package qbft_test

import (
	"context"
	"fmt"
	"github.com/corverroos/qbft"
	"math/rand"
	"runtime"
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

	var (
		ctx, cancel = context.WithCancel(context.Background())
		clock       fakeClock
		receives    []chan qbft.Msg
		broadcast   = make(chan qbft.Msg)
		resultChan  = make(chan string, n)
	)
	defer cancel()

	defs := qbft.Defs{
		IsLeader: func(instance, round int64, process int64) bool {
			return (instance+round)%n == process
		},
		NewTimer: func(round int64) (<-chan time.Time, func()) {
			return clock.NewTimer(time.Second)
		},
		IsValid: func(instance int64, msg qbft.Msg) bool {
			return true
		},
		LogUponRule: func(instance, process int64, msg qbft.Msg, rule string) {
			t.Logf("%d -> %v -> %v ~= %v", msg.Source, msg.Type, process, rule)
		},
		Quorum: q,
		Faulty: f,
	}

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
		runtime.Gosched()
		select {
		case msg := <-broadcast:
			t.Logf("%v -> %v", msg.Source, msg.Type)
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
		default:
			clock.Advance(time.Millisecond * 10)
		}
	}
}
