package raft

import "github.com/xqueries/xdb/internal/id"

// becomeFollower converts a leader to a follower.
// After this function is executed, the node goes back to the loop in raft.go,
// thus resuming normal operations.
func (node *Node) becomeFollower(leaderTerm int32, leaderID id.ID) {
	node.log.
		Debug().
		Str("self-id", node.PersistentState.SelfID.String()).
		Msg("becoming follower")
	node.PersistentState.mu.Lock()
	node.PersistentState.CurrentTerm = leaderTerm
	node.PersistentState.LeaderID = leaderID
	node.PersistentState.VotedFor = leaderID
	node.State = StateFollower.String()
	node.PersistentState.mu.Unlock()
}
