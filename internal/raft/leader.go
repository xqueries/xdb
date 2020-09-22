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
			if s.node.Closed {
				s.lock.Unlock()
				return
			}
			s.node.PersistentState.mu.Lock()
			if s.node.State != StateLeader.String() {
				s.node.PersistentState.mu.Unlock()
				return
			}
			s.node.PersistentState.mu.Unlock()

			s.sendHeartBeats(selfID)
			s.lock.Unlock()

		}
	}()
}

func (s *SimpleServer) sendHeartBeats(selfIDString string) {
	ctx := context.TODO()

	s.node.PersistentState.mu.Lock()
	savedCurrentTerm := s.node.PersistentState.CurrentTerm
	s.node.PersistentState.mu.Unlock()

	// Parallely send AppendEntriesRPC to all followers.
	for i := range s.node.PersistentState.PeerIPs {
		s.node.log.
			Debug().
			Str("self-id", selfIDString).
			Msg("sending heartbeats")
		go func(i int) {
			s.node.PersistentState.mu.Lock()

			nextIndex := s.node.VolatileStateLeader.NextIndex[i]
			prevLogIndex := nextIndex
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = int(s.node.PersistentState.Log[prevLogIndex].Term)
			}
			commitIndex := s.node.VolatileState.CommitIndex
			conn := s.node.PersistentState.PeerIPs[i]
			selfID := s.node.PersistentState.SelfID
			// Logs are included from the nextIndex value to the current appended values
			// in the leader node. If there are none, no logs will be appended.
			var entries []*message.LogData
			if nextIndex >= 0 {
				entries = s.node.PersistentState.Log[nextIndex:]
			}
			s.node.PersistentState.mu.Unlock()

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
				s.node.log.
					Err(err).
					Str("Node", selfIDString).
					Msg("error")
				return
			}

			err = conn.Send(ctx, payload)
			if err != nil {
				s.node.log.
					Err(err).
					Str("Node", selfIDString).
					Msg("error")
				return
			}

			s.node.log.
				Debug().
				Str("self-id", selfIDString).
				Str("sent to", conn.RemoteID().String()).
				Msg("sent heartbeat to peer")

			if s.onAppendEntriesRequest != nil {
				s.onAppendEntriesRequest(conn)
			}

		}(i)
	}
}
