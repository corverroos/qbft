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

func TestQBFT(t *testing.T) {
	t.Run("happy-0", func(t *testing.T) {
		testQBFT(t, test{
			Instance: 0,
			Delay:    nil,
			Result:   1,
		})
	})

	t.Run("happy-1", func(t *testing.T) {
		testQBFT(t, test{
			Instance: 1,
			Delay:    nil,
			Result:   2,
		})
	})

	t.Run("leader-late", func(t *testing.T) {
		testQBFT(t, test{
			Instance: 0,
			Delay:    map[int64]time.Duration{1: time.Second * 2},
			Result:   2,
		})
	})
}

type test struct {
	Instance int64
	Delay    map[int64]time.Duration
	Result   int
}

func testQBFT(t *testing.T, test test) {
	const (
		n = 4
		q = 3
		f = 1
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
			d := time.Duration(round)
			return clock.NewTimer((d * d) * time.Second)
		},
		IsValid: func(instance int64, msg qbft.Msg) bool {
			return true
		},
		LogUponRule: func(instance, process, round int64, msg qbft.Msg, rule string) {
			t.Logf("%d -> %v@%d -> %v@%d ~= %v", msg.Source, msg.Type, msg.Round, process, round, rule)
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
			if d, ok := test.Delay[i]; ok {
				ch, _ := clock.NewTimer(d)
				<-ch
			}

			result, err := qbft.Run(ctx, defs, trans, test.Instance, i, []byte(fmt.Sprint(i)))
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
			t.Logf("%v -> %v@%d", msg.Source, msg.Type, msg.Round)
			for _, out := range receives {
				out <- msg
				if rand.Float64() < 0.1 { // Send 10% messages twice
					out <- msg
				}
			}
		case result := <-resultChan:
			if result != fmt.Sprint(test.Result) {
				t.Fatalf("unexpected result: %v vs %v", result, test.Result)
				return
			}
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
