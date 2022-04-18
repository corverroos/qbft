// Package qbft is a PoC implementation of the https://arxiv.org/pdf/2002.03613.pdf paper
// referenced by the QBFT spec https://github.com/ConsenSys/qbft-formal-spec-and-verification.
package qbft

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"
)

// Transport abstracts the transport layer between processes in the consensus system.
type Transport struct {
	// Broadcast sends the message to all other
	// processes in the system (including this process).
	Broadcast func(Msg)

	// Receive returns a stream of messages received
	// from other processes in the system (including this process).
	Receive <-chan Msg
}

// Defs defines the consensus system parameters that are external to the qbft algorithm.
// This remains constant across multiple instances of consensus (calls to Run).
type Defs struct {
	// IsLeader is a deterministic leader election function.
	IsLeader func(instance, round, process int64) bool
	// NewTimer returns a new timer channel and stop function for the round.
	NewTimer func(round int64) (<-chan time.Time, func())
	// IsValid validates messages.
	IsValid func(instance int64, msg Msg) bool
	// LogUponRule allows debug logging of triggered upon rules on message receipt.
	LogUponRule func(instance, process, round int64, msg Msg, uponRule string)
	// Quorum is the quorum count for the system.
	Quorum int
	// Faulty is the maximum faulty process count for the system.
	Faulty int
}

//go:generate stringer -type=MsgType

// MsgType defines the QBFT message types.
type MsgType int64

const (
	MsgPrePrepare MsgType = iota + 1
	MsgPrepare
	MsgCommit
	MsgRoundChange
)

// Msg defines the inter process messages.
type Msg struct {
	Type          MsgType
	Instance      int64
	Source        int64
	Round         int64
	Value         []byte
	PreparedRound int64
	PreparedValue []byte
}

//go:generate stringer -type=uponRule -trimprefix=upon

// uponRule defines the event based rules that are triggered when messages are received.
type uponRule int64

const (
	uponUnknown uponRule = iota
	uponValidPrePrepare
	uponQuorumPrepare
	uponQuorumCommit
	uponMinRoundChange
	uponQuorumRoundChange
)

// Run returns the consensus decided value (Qcommit) or a context closed error.
func Run(ctx context.Context, d Defs, t Transport, instance, process int64, inputValue []byte) ([]byte, error) {
	if inputValue == nil {
		return nil, errors.New("nil input value not supported")
	}

	// === Helpers ==

	// broadcastMsg broadcasts a non-round-change message.
	broadcastMsg := func(typ MsgType, round int64, value []byte) {
		t.Broadcast(Msg{
			Type:     typ,
			Instance: instance,
			Source:   process,
			Round:    round,
			Value:    value,
		})
	}

	// broadcastRoundChange broadcasts a round-change message.
	broadcastRoundChange := func(round int64, pr int64, pv []byte) {
		t.Broadcast(Msg{
			Type:          MsgRoundChange,
			Instance:      instance,
			Source:        process,
			Round:         round,
			PreparedRound: pr,
			PreparedValue: pv,
		})
	}

	// === State ===

	var (
		round         int64 = 1
		preparedRound int64
		preparedValue []byte
		msgs          []Msg
		dedup         = make(map[dedupKey]bool)
		timerChan     <-chan time.Time
		stopTimer     func()
	)

	// === Algrithm ===

	{ // Algorithm 1.11
		if d.IsLeader(instance, round, process) {
			broadcastMsg(MsgPrePrepare, round, inputValue)
		}

		timerChan, stopTimer = d.NewTimer(round)
	}

	// Handle events until finished.
	for {
		select {
		case msg := <-t.Receive:
			if dedup[key(msg)] {
				continue
			}
			dedup[key(msg)] = true

			if !d.IsValid(instance, msg) {
				continue
			}

			msgs = append(msgs, msg)

			rule, ok := classify(d, instance, round, process, msgs, msg)
			if !ok {
				continue
			}

			d.LogUponRule(instance, process, round, msg, rule.String())

			switch rule {
			case uponValidPrePrepare: // Algorithm 2.1
				stopTimer()
				timerChan, stopTimer = d.NewTimer(round)

				broadcastMsg(MsgPrepare, msg.Round, msg.Value)

			case uponQuorumPrepare: // Algorithm 2.4
				preparedRound = msg.Round
				preparedValue = msg.Value
				broadcastMsg(MsgCommit, msg.Round, msg.Value)

			case uponQuorumCommit: // Algorithm 2.8
				stopTimer()

				return msg.Value, nil

			case uponMinRoundChange: // Algorithm 3.5
				round = getMinRound(d, msgs, round)

				stopTimer()
				timerChan, stopTimer = d.NewTimer(round)

				broadcastRoundChange(round, preparedRound, preparedValue)

			case uponQuorumRoundChange: // Algorithm 3.11
				qrc := filterRoundChange(msgs, msg.Round)
				_, pv := highestPrepared(qrc)

				value := pv
				if value == nil {
					value = inputValue
				}

				broadcastMsg(MsgPrePrepare, round, value)
			default:
				panic("bug: invalid rule")
			}
		case <-timerChan: // Algorithm 3.1
			round++

			stopTimer()
			timerChan, stopTimer = d.NewTimer(round)

			broadcastRoundChange(round, preparedRound, preparedValue)
		case <-ctx.Done():
			// Timeout
			return nil, ctx.Err()
		}
	}
}

// classify returns any rule triggered upon receipt of the last message.
func classify(d Defs, instance, round, process int64, msgs []Msg, msg Msg) (uponRule, bool) {
	// TODO(corver): Figure out how to handle out of sync round messages...
	switch msg.Type {
	case MsgPrePrepare:
		if msg.Round != round {
			return uponUnknown, false
		}
		if justifyPrePrepare(d, instance, msgs, msg) {
			return uponValidPrePrepare, true
		}
	case MsgPrepare:
		if msg.Round != round {
			return uponUnknown, false
		}
		prepareCount := countByRoundAndValue(msgs, MsgPrepare, msg.Round, msg.Value)
		if prepareCount == d.Quorum {
			return uponQuorumPrepare, true
		}
	case MsgCommit:
		commitCount := countByRoundAndValue(msgs, MsgCommit, msg.Round, msg.Value)
		if commitCount == d.Quorum {
			// TODO(corver): Ensure no round check is ok.
			return uponQuorumCommit, true
		}
	case MsgRoundChange:
		changeCount := countByRound(msgs, MsgRoundChange, msg.Round)
		if msg.Round > round && changeCount == d.Faulty+1 {
			return uponMinRoundChange, true
		}

		if msg.Round == round &&
			changeCount == d.Quorum &&
			d.IsLeader(instance, msg.Round, process) &&
			justifyRoundChange(d, msgs, msg) {
			return uponQuorumRoundChange, true
		}

		return uponUnknown, false
	default:
		panic("bug: invalid type")
	}

	return uponUnknown, false
}

func highestPrepared(qrc []Msg) (int64, []byte) { // Algorithm 4.5
	if len(qrc) == 0 {
		// Expect: len(Qrc) >= quorum
		panic("bug: qrc empty")
	}

	// ⊲ Helper function that returns a tuple (pr, pv) where pr and pv are, respectively, the prepared round
	// and the prepared value of the ROUND-CHANGE message in Qrc with the highest prepared round

	var (
		pr int64
		pv []byte
	)
	for _, msg := range qrc {
		if pr < msg.PreparedRound {
			pr = msg.PreparedRound
			pv = msg.PreparedValue
		}
	}

	return pr, pv
}

func getMinRound(d Defs, msgs []Msg, round int64) int64 { // Algorithm 3.6
	// Get all RoundChange messages with round (rj) higher than current round (ri)
	var frc []Msg
	for _, msg := range filterMsgs(msgs, MsgRoundChange, nil, nil, nil, nil) {
		if msg.Round <= round {
			continue
		}
		frc = append(frc, msg)
	}

	// Sanity check
	if len(frc) < d.Faulty+1 {
		panic("bug: too few round change messages")
	}

	// Get the smallest round in the set.
	rmin := int64(math.MaxInt64)
	for _, msg := range frc {
		if rmin > msg.Round {
			rmin = msg.Round
		}
	}

	return rmin
}

func justifyRoundChange(d Defs, msgs []Msg, msg Msg) bool { // Algorithm 4.1
	if msg.Type != MsgRoundChange {
		panic("bug: not a round change message")
	}

	qrc := filterRoundChange(msgs, msg.Round)
	if len(qrc) < d.Quorum {
		return false
	}

	if qrcNoPrepared(qrc) {
		return true
	}

	_, ok := qrcHighestPrepared(d, msgs, qrc)
	if !ok {
		return false
	}

	return true
}

func justifyPrePrepare(d Defs, instance int64, msgs []Msg, msg Msg) bool { // Algorithm 4.3
	if msg.Type != MsgPrePrepare {
		panic("bug: not d preprepare message")
	}

	if !d.IsLeader(instance, msg.Round, msg.Source) {
		return false
	}

	// predicate JustifyPrePrepare((PRE-PREPARE, λi, round, value))
	{
		// round = 1
		if msg.Round == 1 {
			return true
		}
	}
	{
		qrc := filterRoundChange(msgs, msg.Round)
		if len(qrc) < d.Quorum {
			return false
		}

		if qrcNoPrepared(qrc) {
			return true
		}

		pv, ok := qrcHighestPrepared(d, msgs, qrc)
		if !ok {
			return false
		} else if !bytes.Equal(pv, msg.Value) {
			return false
		}

		return true
	}
}

func qrcNoPrepared(qrc []Msg) bool { // Condition J1
	// ∀(ROUND-CHANGE, λi , round, prj , pvj) ∈ Qrc : prj = ⊥ ∧ prj = ⊥
	for _, msg := range qrc {
		if msg.Type != MsgRoundChange {
			panic("bug: invalid Qrc set")
		}
		if msg.PreparedRound != 0 || msg.PreparedValue != nil {
			return false
		}
	}
	return true
}

func qrcHighestPrepared(d Defs, all []Msg, qrc []Msg) ([]byte, bool) { // Condition J2
	pr, pv := highestPrepared(qrc)
	if pr == 0 {
		return nil, false
	}

	if countByRoundAndValue(all, MsgPrepare, pr, pv) < d.Quorum {
		return nil, false
	}

	return pv, true
}

func countByRound(msgs []Msg, typ MsgType, round int64) int {
	return len(filterMsgs(msgs, typ, &round, nil, nil, nil))
}

func countByRoundAndValue(msgs []Msg, typ MsgType, round int64, value []byte) int {
	return len(filterMsgs(msgs, typ, &round, &value, nil, nil))
}

func filterRoundChange(msgs []Msg, round int64) []Msg {
	return filterMsgs(msgs, MsgRoundChange, &round, nil, nil, nil)
}

func filterMsgs(msgs []Msg, typ MsgType, round *int64, value *[]byte, pr *int64, pv *[]byte) []Msg {
	var resp []Msg
	for _, msg := range msgs {
		// Check type
		if typ != msg.Type {
			continue
		}

		// Check round
		if round != nil && *round != msg.Round {
			continue
		}

		// Check value
		if value != nil && !bytes.Equal(*value, msg.Value) {
			continue
		}

		// Check prepared value
		if pv != nil && !bytes.Equal(*pv, msg.PreparedValue) {
			continue
		}

		// Check prepared value
		if pr != nil && *pr != msg.PreparedRound {
			continue
		}

		resp = append(resp, msg)
	}

	return resp
}

func key(msg Msg) dedupKey {
	return dedupKey{
		Source: msg.Source,
		Type:   msg.Type,
	}
}

// dedupKey provides the key to dedups received messages.
type dedupKey struct {
	Source int64
	Type   MsgType
}
