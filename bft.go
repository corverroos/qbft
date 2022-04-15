// Package qbft is a PoC implementation of https://arxiv.org/pdf/2002.03613.pdf.
package qbft

import (
	"bytes"
	"errors"
	"math"
	"time"
)

type (
	ProcessID  int64 // pi
	InstanceID int64 // λi

	State struct {
		deps          Deps
		process       ProcessID
		instance      InstanceID
		round         int64
		preparedRound int64
		preparedValue []byte
		inputValue    []byte
		timer         *time.Timer
		msgs          []Msg
	}

	Deps struct {
		Leader    func(instance InstanceID, round int64) ProcessID
		Timeout   func(round int64) time.Duration
		Decide    func(instance InstanceID, value []byte)
		Valid     func(Msg) bool
		Quorum    int
		Faulty    int
		Broadcast chan<- Msg
		Receive   <-chan Msg
	}

	MsgType int64

	Msg struct {
		Type          MsgType
		Instance      InstanceID
		Source        ProcessID
		Round         int64
		Value         []byte
		PreparedRound int64
		PreparedValue []byte
	}

	Filter struct {
		Type          *MsgType
		Source        *ProcessID
		Round         *int64
		Value         *[]byte
		PreparedRound *int64
		PreparedValue *[]byte
	}

	UponRules int64
)

const (
	MsgPrePrepare MsgType = iota + 1
	MsgPrepare
	MsgCommit
	MsgRoundChange

	UponValidPrePrepare UponRules = iota + 1
	UponQuorumPrepare
	UponQuorumCommit
	UponMinRoundChange
	UponQuorumRoundChange
)

func New(deps Deps, process ProcessID, instance InstanceID, inputValue []byte) (*State, error) {
	if inputValue == nil {
		return nil, errors.New("nil input value not supported")
	}

	return &State{
		deps:       deps,
		process:    process,
		instance:   instance,
		round:      1,
		inputValue: inputValue,
	}, nil
}

func Run(s *State) {
	{ // Algorithm 1.11
		if s.deps.Leader(s.instance, s.round) == s.process {
			broadcastMsg(s, MsgPrePrepare, s.round, s.inputValue)
		}

		resetTimer(s)
	}

	for {
		select {
		case msg := <-s.deps.Receive:
			rules, ok := classify(s, msg)
			if !ok {
				continue
			}

			s.msgs = append(s.msgs, msg)

			for _, rule := range rules {
				switch rule {
				case UponValidPrePrepare: // Algorithm 2.1
					resetTimer(s)
					broadcastMsg(s, MsgPrepare, msg.Round, msg.Value)

				case UponQuorumPrepare: // Algorithm 2.4
					s.preparedRound = msg.Round
					s.preparedValue = msg.Value
					broadcastMsg(s, MsgCommit, msg.Round, msg.Value)

				case UponQuorumCommit: // Algorithm 2.8
					stopTimer(s)
					s.deps.Decide(s.instance, msg.Value)

					return

				case UponMinRoundChange: // Algorithm 3.5
					s.round = getMinRound(s)
					resetTimer(s)
					broadcastRoundChange(s)

				case UponQuorumRoundChange: // Algorithm 3.11
					var value []byte
					if _, pv := highestPrepared(filterMsgs(s.msgs, qrcFilter(s.round))); pv != nil {
						value = s.inputValue
					}
					broadcastMsg(s, MsgPrePrepare, s.round, value)
				}
			}
		case <-s.timer.C: // Algorithm 3.1
			s.round++
			resetTimer(s)
			broadcastRoundChange(s)
		}
	}
}

func classify(s *State, msg Msg) ([]UponRules, bool) {
	if s.instance != msg.Instance {
		return nil, false
	}
	if !s.deps.Valid(msg) {
		return nil, false
	}

	// TODO(corver): Figure out how to handle out of sync round messages...
	var (
		resp    []UponRules
		inclNew = append([]Msg{msg}, s.msgs...)
	)
	switch msg.Type {
	case MsgPrePrepare:
		if justifyPrePrepare(s, msg) {
			resp = append(resp, UponValidPrePrepare)
		}
	case MsgPrepare:
		prepareCount := countMsgs(inclNew, Filter{Type: tPtr(MsgPrepare), Round: iPtr(msg.Round), Value: bPtr(msg.Value)})
		if prepareCount == s.deps.Quorum {
			resp = append(resp, UponQuorumPrepare)
		}
	case MsgCommit:
		commitCount := countMsgs(inclNew, Filter{Type: tPtr(MsgCommit), Round: iPtr(msg.Round), Value: bPtr(msg.Value)})
		if commitCount == s.deps.Quorum {
			resp = append(resp, UponQuorumCommit)
		}
	case MsgRoundChange:
		changeCount := countMsgs(inclNew, Filter{Type: tPtr(MsgRoundChange), Round: iPtr(msg.Round)})
		if changeCount == s.deps.Faulty+1 {
			resp = append(resp, UponMinRoundChange)
		}
		if changeCount == s.deps.Quorum &&
			s.deps.Leader(s.instance, msg.Round) == s.process &&
			justifyRoundChange(s, msg) {
			resp = append(resp, UponQuorumRoundChange)
		}
	}

	return resp, true
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

func getMinRound(s *State) int64 {
	ri := s.round

	countByRound := make(map[int64]int)
	for _, msg := range filterMsgs(s.msgs, Filter{Type: tPtr(MsgRoundChange)}) {
		if msg.Round <= ri {
			continue
		}
		countByRound[msg.Round]++
	}

	rmin := int64(math.MaxInt64)
	for round, count := range countByRound {
		if count < s.deps.Faulty+1 {
			continue
		}
		if rmin > round {
			continue
		}
		rmin = round
	}

	if rmin <= ri {
		panic("bug: no rmin")
	}

	return rmin
}

func justifyRoundChange(s *State, msg Msg) bool { // Algorithm 4.1
	if msg.Type != MsgRoundChange {
		panic("bug: not a round change message")
	}

	inclNew := append([]Msg{msg}, s.msgs...)
	qrc := filterMsgs(inclNew, qrcFilter(msg.Round))
	if len(qrc) < s.deps.Quorum {
		return false
	}

	if qrcNoPrepared(qrc, msg.Round) {
		return true
	}

	_, ok := qrcHighestPrepared(s, qrc)
	if !ok {
		return false
	}

	return true
}

func justifyPrePrepare(s *State, msg Msg) bool { // Algorithm 4.3
	if msg.Type != MsgPrePrepare {
		panic("bug: not a preprepare message")
	}

	if s.deps.Leader(s.instance, msg.Round) != msg.Source {
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
		qrc := filterMsgs(s.msgs, qrcFilter(msg.Round))
		if len(qrc) < s.deps.Quorum {
			return false
		}

		if qrcNoPrepared(qrc, msg.Round) {
			return true
		}

		pv, ok := qrcHighestPrepared(s, qrc)
		if !ok {
			return false
		} else if !bytes.Equal(pv, msg.Value) {
			return false
		}

		return true
	}
}

func qrcNoPrepared(qrc []Msg, round int64) bool {
	// ∀(ROUND-CHANGE, λi , round, prj , pvj) ∈ Qrc : prj = ⊥ ∧ prj = ⊥
	f := qrcFilter(round)
	f.PreparedRound = iPtr(0)
	f.PreparedValue = bPtr(nil)
	noPrepared := filterMsgs(qrc, f)
	return len(noPrepared) == len(qrc)
}

func qrcHighestPrepared(s *State, qrc []Msg) ([]byte, bool) {
	pr, pv := highestPrepared(qrc)
	if pr == 0 {
		return nil, false
	}

	prepFilter := Filter{Type: tPtr(MsgPrepare), Round: iPtr(pr), Value: bPtr(pv)}
	if len(filterMsgs(s.msgs, prepFilter)) < s.deps.Quorum {
		return nil, false
	}

	return pv, true
}

func qrcFilter(round int64) Filter {
	return Filter{Type: tPtr(MsgRoundChange), Round: iPtr(round)}
}

func broadcastMsg(s *State, typ MsgType, round int64, value []byte) {
	s.deps.Broadcast <- Msg{
		Type:     typ,
		Instance: s.instance,
		Source:   s.process,
		Round:    round,
		Value:    value,
	}
}

func broadcastRoundChange(s *State) {
	s.deps.Broadcast <- Msg{
		Type:          MsgRoundChange,
		Instance:      s.instance,
		Source:        s.process,
		Round:         s.round,
		PreparedRound: s.preparedRound,
		PreparedValue: s.preparedValue,
	}
}

func resetTimer(s *State) {
	stopTimer(s)
	s.timer = time.NewTimer(s.deps.Timeout(s.round))
}

func stopTimer(s *State) {
	if s.timer == nil {
		return
	}

	if !s.timer.Stop() {
		<-s.timer.C
	}
	s.timer = nil
}

func countMsgs(msgs []Msg, filter Filter) int {
	return len(filterMsgs(msgs, filter))
}

func filterMsgs(msgs []Msg, filter Filter) []Msg {
	var resp []Msg
	for _, msg := range msgs {
		// Check type
		if filter.Type != nil && *filter.Type != msg.Type {
			continue
		}

		// Check round
		if filter.Round != nil && *filter.Round != msg.Round {
			continue
		}

		// Check value
		if filter.Value != nil && !bytes.Equal(*filter.Value, msg.Value) {
			continue
		}

		// Check prepared value
		if filter.PreparedValue != nil && !bytes.Equal(*filter.PreparedValue, msg.PreparedValue) {
			continue
		}

		// Check prepared value
		if filter.PreparedRound != nil && *filter.PreparedRound != msg.PreparedRound {
			continue
		}

		resp = append(resp, msg)
	}

	return resp
}

func tPtr(typ MsgType) *MsgType {
	return &typ
}

func iPtr(i int64) *int64 {
	return &i
}
func bPtr(b []byte) *[]byte {
	return &b
}
