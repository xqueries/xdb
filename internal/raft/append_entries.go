package raft

import (
	"log"

	"github.com/xqueries/xdb/internal/id"
	"github.com/xqueries/xdb/internal/raft/message"
)

// AppendEntriesResponse function is called on a request from the leader to append log data
// to the follower node. This function generates the response to be sent to the leader node.
// This is the response to the contact by the leader to assert it's leadership.
func (s *SimpleServer) AppendEntriesResponse(req *message.AppendEntriesRequest) *message.AppendEntriesResponse {
	// leader term is just "term" in the paper, but since
	// they mean the leader's term, we choose to use this.
	leaderTerm := req.GetTerm()
	s.lock.Lock()
	if s.node.Closed {
		s.lock.Unlock()
		log.Println("node was closed, returning")
		return nil
	}
	s.lock.Unlock()

	s.node.PersistentState.mu.Lock()
	nodePersistentState := s.node.PersistentState
	currentTerm := nodePersistentState.CurrentTerm
	selfID := s.node.PersistentState.SelfID.String()
	s.node.PersistentState.mu.Unlock()

	// Return false if the leader's term is lesser than currentTerm,
	// because it means that the leader is in a stale state.
	//
	// TODO: Still confused - if msg Log Index is greater than node commit Index.
	//
	// Return false if term of leader in PrevLogIndex doesn't match
	// the previous Log Term stored by Leader.
	if len(nodePersistentState.Log) > 0 && (leaderTerm < currentTerm ||
		req.GetPrevLogIndex() > s.node.VolatileState.CommitIndex ||
		nodePersistentState.Log[req.GetPrevLogIndex()].Term != req.GetPrevLogTerm()) {
		s.node.log.
			Debug().
			Str("self-id", selfID).
			Str("returning failure to append entries to", string(req.GetLeaderID())).
			Msg("append entries failure")
		return &message.AppendEntriesResponse{
			Term:          currentTerm,
			Success:       false,
			EntriesLength: 0,
		}
	}

	if leaderTerm > currentTerm {
		s.node.log.Debug().
			Str("self-id", selfID).
			Msg("self term out of date, returning to follower state")
		leaderID, err := id.Parse(req.GetLeaderID())
		if err != nil {
			log.Printf("error in parsing leader ID in AppendEntriesResponse: %v\n", err)
		}
		s.node.becomeFollower(leaderTerm, leaderID)
	}

	entries := req.GetEntries()
	if len(entries) > 0 {
		nodePersistentState.mu.Lock()
		if req.GetPrevLogIndex() < s.node.VolatileState.CommitIndex {
			s.node.PersistentState.Log = s.node.PersistentState.Log[:req.GetPrevLogIndex()]
		}
		s.node.PersistentState.Log = append(s.node.PersistentState.Log, entries...)
		s.node.PersistentState.mu.Unlock()
	}

	if req.GetLeaderCommit() > s.node.VolatileState.CommitIndex {
		nodeCommitIndex := req.GetLeaderCommit()
		if int(req.GetLeaderCommit()) > len(s.node.PersistentState.Log) {
			nodeCommitIndex = int32(len(s.node.PersistentState.Log))
		}
		s.node.VolatileState.CommitIndex = nodeCommitIndex
		/* FIX ISSUE #152 from this
		commandEntries := getCommandFromLogs(entries)
		succeeded := s.onReplication(commandEntries)
		_ = succeeded
		// succeeded returns the number of applied entries.
		*/
	}

	s.node.log.
		Debug().
		Str("self-id", s.node.PersistentState.SelfID.String()).
		Str("returning success to append entries to", string(req.GetLeaderID())).
		Msg("append entries success")

	if s.onAppendEntriesResponse != nil {
		s.onAppendEntriesResponse()
	}

	return &message.AppendEntriesResponse{
		Term:          currentTerm,
		Success:       true,
		EntriesLength: int32(len(req.Entries)),
	}

}

// func getCommandFromLogs(entries []*message.LogData) []*message.Command {
// 	var commandEntries []*message.Command
// 	for i := range entries {
// 		commandEntries = append(commandEntries, entries[i].Entry)
// 	}
// 	return commandEntries
// }
