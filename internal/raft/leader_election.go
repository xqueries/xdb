package raft

import (
	"context"
	"fmt"

	"github.com/xqueries/xdb/internal/raft/message"
)

// StartElection enables a node in the cluster to start the election.
// The function caller doesn't need to wait for a voting response from this function,
// the function triggers the necessary functions responsible to continue the raft cluster
// into it's working stage if the node won the election.
func (s *SimpleServer) StartElection(ctx context.Context) {

	s.lock.Lock()
	s.node.PersistentState.mu.Lock()
	s.node.PersistentState.CurrentTerm++
	s.node.State = StateCandidate.String()
	var lastLogTerm, lastLogIndex int32
	savedCurrentTerm := s.node.PersistentState.CurrentTerm
	if len(s.node.PersistentState.Log) == 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = s.node.PersistentState.Log[len(s.node.PersistentState.Log)].Term
	}
	lastLogIndex = int32(len(s.node.PersistentState.Log))
	selfID := s.node.PersistentState.SelfID
	numNodes := s.node.PersistentState.PeerIPs
	s.node.log.
		Debug().
		Str("self-id", selfID.String()).
		Int32("term", s.node.PersistentState.CurrentTerm).
		Msg("starting election")
	s.node.PersistentState.mu.Unlock()
	s.lock.Unlock()

	for i := range numNodes {
		// Parallelly request votes from the peers.
		go func(i int) {
			req := message.NewRequestVoteRequest(
				savedCurrentTerm,
				selfID,
				lastLogIndex,
				lastLogTerm,
			)
			s.lock.Lock()
			if s.node.Closed {
				return
			}
			nodeConn := s.node.PersistentState.PeerIPs[i]
			s.lock.Unlock()

			err := s.RequestVote(ctx, nodeConn, req)
			if err != nil {
				fmt.Println(err)
			}
			// If there's an error, the vote is considered to be not casted by the node.
			// Worst case, there will be a re-election; the errors might be from network or
			// data consistency errors, which will be sorted by a re-election.
			// This decision was taken because, StartElection returning an error is not feasible.
		}(i)
	}
}
