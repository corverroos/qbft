package qbft_test

import (
	"fmt"
	"github.com/corverroos/qbft"
	"testing"
	"time"
)

func TestHappy(t *testing.T) {
	const (
		n = 4
		q = 3
		f = 1

		height = 100 // Vary this see the results change
	)

	resultChan := make(chan string, n)

	busIn := make(chan qbft.Msg)

	deps := qbft.Deps{
		Leader: func(instance qbft.InstanceID, round int64) qbft.ProcessID {
			return qbft.ProcessID((int64(instance) + round) % n)
		},
		Timeout: func(round int64) time.Duration {
			return time.Second
		},
		Decide: func(_ qbft.InstanceID, value []byte) {
			resultChan <- string(value)
		},
		Valid: func(qbft.Msg) bool {
			return true
		},
		Quorum:    q,
		Faulty:    f,
		Broadcast: busIn,
		Receive:   nil,
	}

	var busOuts []chan qbft.Msg
	for i := 0; i < n; i++ {
		busOut := make(chan qbft.Msg, 1000)
		busOuts = append(busOuts, busOut)
		deps.Receive = busOut

		s, err := qbft.New(deps, qbft.ProcessID(i), height, []byte(fmt.Sprint(i)))
		if err != nil {
			t.Fatal(err)
		}

		go func(s *qbft.State, i int) {
			qbft.Run(s)
		}(s, i)
	}

	var results []string

	for {
		select {
		case msg := <-busIn:
			for _, out := range busOuts {
				out <- msg
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
