package raft

import (
	"fmt"
	"log"

	"github.com/xqueries/xdb/internal/raft/message"
)

// AppendEntriesResponse function is called by a follower on a request from the
// leader to append log data to the follower node. This function generates the
// response to be sent to the leader node. This is the response to the request
// from the leader to assert it's leadership.
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

	s.node.VolatileState.mu.Lock()
	commitIndex := s.node.VolatileState.CommitIndex
	s.node.VolatileState.mu.Unlock()

	// Return false if the leader's term is lesser than currentTerm,
	// because it means that the leader is in a stale state.
	isLeaderStale := leaderTerm < currentTerm

	// Bound check variable for future operations on the logs.
	s.node.PersistentState.mu.Lock()
	logsToExecuteExist := len(s.node.PersistentState.Log) > 0
	s.node.PersistentState.mu.Unlock()

	// Return false if term of leader in PrevLogIndex doesn't atleast
	// match or exceed the previous Log Term stored by Leader. Essentially
	// this is to check whether the leader is not running behind the
	// follower. This case means that "leader" is not the real leader
	// and is behind on the logs.
	//
	// In these cases, a true leader would be elected and followed but this
	// "leader" for any possible reason (network issues, lag) hasn't caught
	// up with it yet.
	isLeaderAheadInLogs := req.GetPrevLogIndex() >= commitIndex

	// Reply false if the current node's log doesn't contain an entry
	// at prevLogIndex whose term matches prevLogTerm (of the incoming node's).
	isPreviousTermMatchingInLeaderAndFollower :=
		logsToExecuteExist && (nodePersistentState.Log[req.GetPrevLogIndex()].Term != req.GetPrevLogTerm())

	/* Criterion for a failed append entries request:
		1. Checking whether there are logs to execute - bounds check for
			future operations on the nature of the logs.
		2. Checking whether the leader is updated and not stale.
		3. Checking whether the leader is not a phantom leader.
		4. Checking whether the last committed logs in leader and
			follower match.
	 */
	isFailedAppendEntriesResponse := isLeaderStale || !isLeaderAheadInLogs ||
		isPreviousTermMatchingInLeaderAndFollower

	if isFailedAppendEntriesResponse {
		var msg string
		if isLeaderStale {
			msg = fmt.Sprintf("leader stale, leader term: %d, current term: %d", leaderTerm, currentTerm)
		}
		if !isLeaderAheadInLogs {
			if msg != "" {
				msg+="\n"
			}
			msg = fmt.Sprintf("leader %v is not ahead in logs ", req.LeaderID)
		}
		s.node.log.
			Debug().
			Str("self-id", selfID).
			Str("returning failure to append entries to", string(req.GetLeaderID())).
			Str("reason",msg).
			Msg("append entries failure")
		return &message.AppendEntriesResponse{
			Term:          currentTerm,
			Success:       false,
			EntriesLength: 0,
		}
	}

	entries := req.GetEntries()
	// Checking for entries in the log.
	//
	// At this point, only failed AppendEntriesRequest's are cleared,
	// we still have to check whether there are entries to be processed,
	// because this can still be a heartbeat.
	if len(entries) > 0 {
		s.node.PersistentState.mu.Lock()

		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it.
		//
		// This is done in order to remove phantom logs or uncommitted
		// logs from the follower. Phantom logs can appear when a leader
		// which appended to the follower crashed before updating the
		// commit index of the follower. Now, the new leader might not
		// have these logs in it and since all followers must behave to
		// the leader, this truncating operation happens.
		/**
			Example:
					Follower.PersistentState.Log -> [1,1,2,2,3,3,3,4,5,6]
					Leader.Entries -> [7,7,7]
					Leader.prevLogIndex -> 6 (this indicates that, 7 entries were committed
		 									  according to this leader)
					Leader.prevLogTerm -> 3 (this matches the term existing in the follower log)
					Now, at index i = 7 of follower log and index j = 0 of the entries,
					there is a mismatch. Thus, all logs starting from this index is removed.

					Follower.PersistentState.Log -> [1,1,2,2,3,3,3]
					After appending new entries -> [1,1,2,2,3,3,3,7,7,7]
		 */
		prevLogIndex := req.GetPrevLogIndex()
		for i := prevLogIndex + 1;
			i < int32(len(s.node.PersistentState.Log));
			i++ {
				if i - prevLogIndex < int32(len(req.GetEntries())) &&
					s.node.PersistentState.Log[i].GetTerm() != req.GetEntries()[i-prevLogIndex].GetTerm() {
					s.node.PersistentState.Log = s.node.PersistentState.Log[i:]
					break
				}
		}

		// Append any new entries not already in the log, entries that came in the request.
		s.node.PersistentState.Log = append(s.node.PersistentState.Log, entries...)
		s.node.PersistentState.mu.Unlock()

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry in follower)
		//
		// This happens post appending entries, this operation is
		// basically catching up the follower with the newly committed
		// indexes. (This probably isn't from this txn - TODO: Check)
		leaderCommitIndex := req.GetLeaderCommit()
		if leaderCommitIndex > commitIndex {
			s.node.VolatileState.mu.Lock()
			if int(leaderCommitIndex) > len(nodePersistentState.Log) {
				s.node.VolatileState.CommitIndex = int32(len(nodePersistentState.Log))
			} else {
				s.node.VolatileState.CommitIndex = leaderCommitIndex
			}
			s.node.VolatileState.mu.Unlock()


			// What I believe is left here is, letting the parent caller functions
			// from the DB that, replication was successful and we can proceed with
			// the operations.
			/* FIX ISSUE #152 from this
			commandEntries := getCommandFromLogs(entries)
			succeeded := s.onReplication(commandEntries)
			_ = succeeded
			// succeeded returns the number of applied entries.
			*/
		}
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
