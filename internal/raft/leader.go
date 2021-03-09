package raft

import (
	"context"
	"time"

	"github.com/xqueries/xdb/internal/raft/message"
)

// startLeader begins the leaders operations. Once a leader is confirmed
// to be elected, this function is executed.
//
// The leader is responsible to do two things; one, ensure that all other
// nodes know that there is a leader alive in this term and two, to send
// logs that were received by the client and maintain consensus. Part one
// is achieved by sending heartbeats when there are no logs that are to
// be appended and two is achieved by sending the AppendEntriesRequest.
//
// The leader spawns a separate goroutine to ensure
// The leader begins by sending append entries RPC to the nodes parallelly.
// The leader sends periodic append entries request to the
// followers to keep them alive.
//
// The selfID is passed as an argument for two reasons,
// one it acts as a double check that this node is actually the leader,
// and second, it reduces locks to find out the selfID in the future.
//
// Empty append entries request are also called heartbeats.
// The data that goes in the append entries request is determined by
// existence of data in the LogChannel channel.
//
// This function doesn't bother with obtaining the response for the sent
// requests. This is handled by the raft-core functions.
func (s *SimpleServer) startLeader(ctx context.Context, selfID string) {

	s.node.log.
		Debug().
		Str("self-id", selfID).
		Msg("starting leader election proceedings")

	stopLeaderOps := make(chan struct{})
	go func() {
		// The loop that the leader stays in until it's functioning properly.
		// The goal of this loop is to maintain raft in it's working phase;
		// periodically sending heartbeats/appendEntries.
		// This loop goes on until this node is the leader.
		for {
			select {
			case <-ctx.Done():
				s.node.log.
					Debug().
					Str("self-id", selfID).
					Msg("leader stopping operation, stop signal encountered")
				stopLeaderOps <- struct{}{}
				return
			default:
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
					stopLeaderOps <- struct{}{}
					return
				}
				s.node.PersistentState.mu.Unlock()

				s.sendHeartBeats(ctx, selfID, stopLeaderOps)
				s.lock.Unlock()
			}
		}
	}()
}

func (s *SimpleServer) sendHeartBeats(ctx context.Context, selfIDString string, stopLeaderOps chan struct{}) {

	s.node.PersistentState.mu.Lock()
	savedCurrentTerm := s.node.PersistentState.CurrentTerm
	s.node.PersistentState.mu.Unlock()

	// Parallelly send AppendEntriesRPC to all followers.
	for i := range s.node.PersistentState.peerIPs {
		select {
		case <-stopLeaderOps:
			s.node.log.
				Debug().
				Str("self-id", selfIDString).
				Msg("leader stopping operation mid-heartbeat, stop signal encountered")
			return
		default:
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
				conn := s.node.PersistentState.peerIPs[i]
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
						Msg("Error marshalling append entries request in leader")
					return
				}

				err = conn.Send(ctx, payload)
				if err != nil {
					s.node.log.
						Err(err).
						Str("Node", selfIDString).
						Msg("Error sending append entries request over network")
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
}
