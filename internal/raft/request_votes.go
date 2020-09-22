package raft

import (
	"context"
	"fmt"

	"github.com/xqueries/xdb/internal/id"
	"github.com/xqueries/xdb/internal/network"
	"github.com/xqueries/xdb/internal/raft/message"
)

// RequestVote enables a node to send out the RequestVotes RPC.
// This function requests a vote from one node and returns that node's response.
// It opens a connection to the intended node using the network layer and waits for a response.
func (s *SimpleServer) RequestVote(ctx context.Context, nodeConn network.Conn, req *message.RequestVoteRequest) error {

	s.lock.Lock()
	selfID := s.node.PersistentState.SelfID
	s.node.log.
		Debug().
		Str("self-id", selfID.String()).
		Str("request-vote sent to", nodeConn.RemoteID().String()).
		Msg("request vote")
	s.lock.Unlock()

	payload, err := message.Marshal(req)
	if err != nil {
		return err
	}

	err = nodeConn.Send(ctx, payload)
	if err != nil {
		return err
	}

	// Set the hook for a request vote completion.
	if s.onRequestVotes != nil {
		s.onRequestVotes(nodeConn)
	}

	return nil
}

// RequestVoteResponse function is called on a request from a candidate for a vote. This function
// generates the response for the responder node to send back to the candidate node.
func (s *SimpleServer) RequestVoteResponse(req *message.RequestVoteRequest) *message.RequestVoteResponse {
	s.lock.Lock()
	s.node.PersistentState.mu.Lock()
	currentTerm := s.node.PersistentState.CurrentTerm
	selfID := s.node.PersistentState.SelfID.String()
	s.node.PersistentState.mu.Unlock()
	s.lock.Unlock()

	// If the candidate is not up to date with the term, reject the vote.
	if req.Term < currentTerm {
		s.node.log.
			Debug().
			Str("leader ID", string(req.CandidateID)).
			Msg("is stale")
		return &message.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	s.node.PersistentState.mu.Lock()
	// If this node hasn't voted for any other node, vote only then.
	// TODO: Check whether candidate's log is atleast as up to date as mine only then grant vote.
	if s.node.PersistentState.VotedFor == nil { //} &&
		// currentTerm == req.GetTerm() {
		cID, err := id.Parse(req.CandidateID)
		if err != nil {
			// no point in handling this because I really need that to parse into ID.
			fmt.Println(err)
		}
		s.node.PersistentState.VotedFor = cID
		s.node.log.
			Debug().
			Str("self-id", selfID).
			Str("vote granted to", cID.String()).
			Msg("voting a peer")

		s.timerReset <- struct{}{}
		s.node.PersistentState.mu.Unlock()
		return &message.RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: true,
		}
	}
	s.node.PersistentState.mu.Unlock()

	return &message.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: false,
	}
}
