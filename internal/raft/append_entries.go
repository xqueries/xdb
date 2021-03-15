package raft

import (
	"log"

	"github.com/xqueries/xdb/internal/raft/message"
)

// AppendEntriesResponse function is called by a follower on a request from the
// leader to append log data to the follower node. This function generates the
// response to be sent to the leader node. This is the response to the contact
// by the leader to assert it's leadership.
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

	// Return false if term of leader in PrevLogIndex doesn't match
	// the previous Log Term stored by Leader. Essentially this is to check
	// whether the leader is not running behind the follower. This case means
	// that "leader" is not the real leader and is behind on the logs.
	//
	// In these cases, a true leader would be elected and followed but this
	// "leader" for any possible reason (network issues, lag) hasn't caught
	// up with it yet.

	isLeaderAheadInLogs := req.GetPrevLogIndex() > commitIndex

	// Return false if the last committed logs don't have the same term.
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
	isFailedAppendEntriesResponse := isLeaderStale || isLeaderAheadInLogs ||
		isPreviousTermMatchingInLeaderAndFollower

	if isFailedAppendEntriesResponse {
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

	entries := req.GetEntries()
	// Checking for entries in the log.
	//
	// At this point, only failed AppendEntriesRequest's are cleared,
	// we still have to check whether there are entries to be processed,
	// because this can still be a heartbeat.
	if len(entries) > 0 {
		s.node.PersistentState.mu.Lock()
		// If there are additional entries to be committed,
		if req.GetPrevLogIndex() < commitIndex {
			// This statement is disregarding all the unchecked logs in this node
			// and cutting them off.
			s.node.PersistentState.Log = s.node.PersistentState.Log[:req.GetPrevLogIndex()]
		}
		// Later, that node gets appended the entries that came in the request.
		s.node.PersistentState.Log = append(s.node.PersistentState.Log, entries...)
		s.node.PersistentState.mu.Unlock()

		/*
			TODO:
				1. If an existing entry conflicts with a new one (same index
					but different terms), delete the existing entry and all that
					follow it. - Looks like theres no option other than checking all indices.
				2.  Append any new entries not already in the log
				3.  If leaderCommit > commitIndex, set commitIndex =
					min(leaderCommit, index of last new entry in follower)
		*/

		leaderCommitIndex := req.GetLeaderCommit()
		if leaderCommitIndex > commitIndex {

			if int(leaderCommitIndex) > len(nodePersistentState.Log) {
				leaderCommitIndex = int32(len(nodePersistentState.Log))
			}

			// TODO: CHANGE BASED ON ALGO
			s.node.VolatileState.CommitIndex = leaderCommitIndex
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
