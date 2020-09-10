package raft

import (
	"context"
	"time"

	"github.com/xqueries/xdb/internal/raft/message"
)

// startLeader begins the leaders operations.
// The selfID is passed as an argument for two reasons,
// one it acts as a double check that this node is inturn the leader,
// and second, tit reduces locks to find out the selfID in the future.
//
// The leader begins by sending append entries RPC to the nodes.
// The leader sends periodic append entries request to the
// followers to keep them alive.
// Empty append entries request are also called heartbeats.
// The data that goes in the append entries request is determined by
// existance of data in the LogChannel channel.
func (s *SimpleServer) startLeader(selfID string) {

	s.node.log.
		Debug().
		Str("self-id", selfID).
		Msg("starting leader election proceedings")

	go func() {
		// The loop that the leader stays in until it's functioning properly.
		// The goal of this loop is to maintain raft in it's working phase;
		// periodically sending heartbeats/appendEntries.
		// This loop goes on until this node is the leader.
		for {
			// Send heartbeats every 50ms.
			<-time.NewTimer(50 * time.Millisecond).C

			// Before continuing the operations, check whether
			// the server is not closed.
			s.lock.Lock()
			if s.node == nil {
				s.lock.Unlock()
				return
			}
			s.node.PersistentState.mu.Lock()
			if s.node.State != StateLeader.String() {
				s.node.PersistentState.mu.Unlock()
				return
			}
			s.node.PersistentState.mu.Unlock()

			s.node.sendHeartBeats(selfID)
			s.lock.Unlock()

			if s.onAppendEntries != nil {
				s.onAppendEntries()
			}
		}
	}()
}

func (node *Node) sendHeartBeats(selfIDString string) {
	ctx := context.TODO()

	node.PersistentState.mu.Lock()
	savedCurrentTerm := node.PersistentState.CurrentTerm
	node.PersistentState.mu.Unlock()

	// Parallely send AppendEntriesRPC to all followers.
	for i := range node.PersistentState.PeerIPs {
		node.log.
			Debug().
			Str("self-id", selfIDString).
			Msg("sending heartbeats")
		go func(i int) {
			node.PersistentState.mu.Lock()

			nextIndex := node.VolatileStateLeader.NextIndex[i]
			prevLogIndex := nextIndex
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = int(node.PersistentState.Log[prevLogIndex].Term)
			}
			commitIndex := node.VolatileState.CommitIndex
			conn := node.PersistentState.PeerIPs[i]
			selfID := node.PersistentState.SelfID
			// Logs are included from the nextIndex value to the current appended values
			// in the leader node. If there are none, no logs will be appended.
			var entries []*message.LogData
			if nextIndex >= 0 {
				entries = node.PersistentState.Log[nextIndex:]
			}
			node.PersistentState.mu.Unlock()

			appendEntriesRequest := message.NewAppendEntriesRequest(
				savedCurrentTerm,
				selfID,
				int32(prevLogIndex),
				int32(prevLogTerm),
				entries,
				commitIndex,
			)

			payload, err := message.Marshal(appendEntriesRequest)
			if err != nil {
				node.log.
					Err(err).
					Str("Node", selfIDString).
					Msg("error")
				return
			}

			err = conn.Send(ctx, payload)
			if err != nil {
				node.log.
					Err(err).
					Str("Node", selfIDString).
					Msg("error")
				return
			}

			node.log.
				Debug().
				Str("self-id", selfIDString).
				Str("sent to", conn.RemoteID().String()).
				Msg("sent heartbeat to peer")

			res, err := conn.Receive(ctx)
			if err != nil {
				node.log.
					Err(err).
					Str("Node", selfIDString).
					Msg("error")
				return
			}

			resP, err := message.Unmarshal(res)
			if err != nil {
				node.log.
					Err(err).
					Str("Node", selfIDString).
					Msg("error")
				return
			}

			appendEntriesResponse := resP.(*message.AppendEntriesResponse)

			// If the term in the other node is greater than this node's term,
			// it means that this node is not up to date and has to step down
			// from being a leader.
			if appendEntriesResponse.Term > savedCurrentTerm {
				node.log.Debug().
					Str(selfIDString, "stale term").
					Str("following newer node", conn.RemoteID().String())
				node.becomeFollower()
				return
			}

			if node.State == StateLeader.String() && appendEntriesResponse.Term == savedCurrentTerm {
				if appendEntriesResponse.Success {
					node.PersistentState.mu.Lock()
					node.VolatileStateLeader.NextIndex[i] = nextIndex + len(entries)
					node.PersistentState.mu.Unlock()
				} else {
					// If this appendEntries request failed,
					// proceed and retry in the next cycle.
					node.log.
						Debug().
						Str("self-id", selfIDString).
						Str("received failure to append entries from", conn.RemoteID().String()).
						Msg("failed to append entries")
				}
			}
		}(i)
	}
}
