package raft

import (
	"context"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xqueries/xdb/internal/id"
	"github.com/xqueries/xdb/internal/network"
	"github.com/xqueries/xdb/internal/raft/message"
)

// Server is a description of a raft server.
type Server interface {
	Start(context.Context) error
	OnReplication(ReplicationHandler)
	Input(*message.Command)
	io.Closer
}

// ReplicationHandler is a handler setter.
// It takes in the log entries as a string and returns the number
// of succeeded application of entries.
type ReplicationHandler func([]*message.Command) int

// Node describes the current state of a raft node.
// The raft paper describes this as a "State" but node
// seemed more intuitive.
type Node struct {
	State string
	log   zerolog.Logger

	PersistentState     *PersistentState
	VolatileState       *VolatileState
	VolatileStateLeader *VolatileStateLeader

	Closed bool
}

// PersistentState describes the persistent state data on a raft node.
type PersistentState struct {
	CurrentTerm int32
	VotedFor    id.ID // VotedFor is nil at init, and id.ID of the node after voting is complete.
	Log         []*message.LogData

	SelfID    id.ID
	LeaderID  id.ID          // LeaderID is nil at init, and the ID of the node after the leader is elected.
	PeerIPs   []network.Conn // PeerIPs has the connection variables of all the other nodes in the cluster.
	ConnIDMap map[id.ID]int  // ConnIDMap has a mapping of the ID of the server to its connection.
	mu        sync.Mutex
}

// VolatileState describes the volatile state data on a raft node.
type VolatileState struct {
	CommitIndex int32
	LastApplied int32
	Votes       int32
}

// VolatileStateLeader describes the volatile state data that exists on a raft leader.
type VolatileStateLeader struct {
	NextIndex  []int // Holds the nextIndex value for each of the followers in the cluster.
	MatchIndex []int // Holds the matchIndex value for each of the followers in the cluster.
}

var _ Server = (*SimpleServer)(nil)

// SimpleServer implements a server in a cluster.
type SimpleServer struct {
	node            *Node
	cluster         Cluster
	onReplication   ReplicationHandler
	log             zerolog.Logger
	timeoutProvider func(*Node) *time.Timer
	lock            sync.Mutex

	onRequestVotes          func(network.Conn)
	onLeaderElected         func()
	onAppendEntriesRequest  func(network.Conn)
	onAppendEntriesResponse func()
	onCompleteOneRound      func()

	timerReset chan struct{}
}

// incomingData describes every request that the server gets.
type incomingData struct {
	conn network.Conn
	msg  message.Message
}

// NewServer enables starting a raft server/cluster.
func NewServer(log zerolog.Logger, cluster Cluster) *SimpleServer {
	return newServer(log, cluster, nil)
}

func newServer(log zerolog.Logger, cluster Cluster, timeoutProvider func(*Node) *time.Timer) *SimpleServer {
	if timeoutProvider == nil {
		timeoutProvider = randomTimer
	}
	resetChan := make(chan struct{}, 2)
	return &SimpleServer{
		log:             log.With().Str("component", "raft").Logger(),
		cluster:         cluster,
		timeoutProvider: timeoutProvider,
		timerReset:      resetChan,
	}
}

// NewRaftNode initialises a raft cluster with the given configuration.
func NewRaftNode(cluster Cluster) *Node {
	var nextIndex, matchIndex []int

	for range cluster.Nodes() {
		nextIndex = append(nextIndex, -1)
		matchIndex = append(matchIndex, -1)
	}

	connIDMap := make(map[id.ID]int)
	for i := range cluster.Nodes() {
		connIDMap[cluster.Nodes()[i].RemoteID()] = i
	}

	node := &Node{
		// All servers start as followers, on timeouts, they become candidates.
		State: StateFollower.String(),
		PersistentState: &PersistentState{
			CurrentTerm: 0,
			VotedFor:    nil,
			SelfID:      cluster.OwnID(),
			PeerIPs:     cluster.Nodes(),
			ConnIDMap:   connIDMap,
		},
		VolatileState: &VolatileState{
			CommitIndex: -1,
			LastApplied: -1,
			Votes:       0,
		},
		VolatileStateLeader: &VolatileStateLeader{
			NextIndex:  nextIndex,
			MatchIndex: matchIndex,
		},
		Closed: false,
	}
	return node
}

// Start starts a single raft node into beginning raft operations.
// This function starts the leader election and keeps a check on whether
// regular heartbeats to the node exists. It restarts leader election on failure to do so.
// This function also continuously listens on all the connections to the nodes
// and routes the requests to appropriate functions.
func (s *SimpleServer) Start(ctx context.Context) (err error) {
	// Making the function idempotent, returns whether the server is already open.
	s.lock.Lock()
	if s.node != nil {
		s.log.Debug().
			Str("self-id", s.node.PersistentState.SelfID.String()).
			Msg("already open")
		s.lock.Unlock()
		return network.ErrOpen
	}
	// Initialise all raft variables in this node.
	node := NewRaftNode(s.cluster)
	node.PersistentState.mu.Lock()
	node.log = s.log
	s.node = node
	selfID := node.PersistentState.SelfID
	node.PersistentState.mu.Unlock()
	s.lock.Unlock()

	// liveChan is a channel that passes the incomingData once received.
	liveChan := make(chan *incomingData)
	// Listen forever on all node connections.
	go func() {
		for {
			// Parallely start waiting for incoming data.
			conn, msg, err := s.cluster.Receive(ctx)
			if err != nil {
				log.Printf("error in receiving from the cluster: %v\n", err)
				return
			}

			if msg != nil {
				node.log.
					Debug().
					Str("self-id", selfID.String()).
					Str("received", msg.Kind().String()).
					Msg("received request")
				liveChan <- newIncomingData(conn, msg)
			}
		}
	}()

	// This block of code checks what kind of request has to be serviced
	// and calls the necessary function to complete it.
	for {
		// If any sort of request (heartbeat,appendEntries,requestVote)
		// isn't received by the server(node) it restarts leader election.
		s.node.PersistentState.mu.Lock()
		if s.node.Closed {
			s.node.PersistentState.mu.Unlock()
			return
		}
		s.node.PersistentState.mu.Unlock()

		select {
		case <-s.timeoutProvider(node).C:
			if s.node.PersistentState.LeaderID == s.node.PersistentState.SelfID {
				break
			}
			s.lock.Lock()
			if s.node.Closed {
				log.Printf("node was closed, exiting")
				s.lock.Unlock()
				return
			}
			// If this node is already the leader the time-outs are irrelevant.
			if s.node.PersistentState.LeaderID != s.node.PersistentState.SelfID {
				// One round is said to be complete when leader election
				// is started for all terms except the first term.
				if s.node.PersistentState.CurrentTerm != 1 && s.onCompleteOneRound != nil {
					s.onCompleteOneRound()
				}
				s.lock.Unlock()
				s.StartElection(ctx)
			} else {
				s.lock.Unlock()
			}
		case data := <-liveChan:
			err = s.processIncomingData(data)
			if err != nil {
				log.Printf("error in processing data: %v\n", err)
				return
			}
		case <-ctx.Done():
			return
		case <-s.timerReset:
			// When a timer reset signal is received, this
			// select loop is restarted, effectively restarting
			// the timer.
		}
	}
}

// OnReplication is a handler setter.
func (s *SimpleServer) OnReplication(handler ReplicationHandler) {
	s.onReplication = handler
}

// Input appends the input log into the leaders log, only if the current node is the leader.
// If this was not a leader, the leaders data is communicated to the client.
func (s *SimpleServer) Input(input *message.Command) {
	s.node.PersistentState.mu.Lock()
	defer s.node.PersistentState.mu.Unlock()

	if s.node.State == StateLeader.String() {
		logData := message.NewLogData(s.node.PersistentState.CurrentTerm, input)
		s.node.PersistentState.Log = append(s.node.PersistentState.Log, logData)
	} else {
		// Relay data to leader.
		logAppendRequest := message.NewLogAppendRequest(input)

		s.relayDataToServer(logAppendRequest)
	}
}

// Close closes the node and returns an error on failure.
func (s *SimpleServer) Close() error {
	s.lock.Lock()
	// Maintaining idempotency of the close function.
	if s.node.Closed {
		return network.ErrClosed
	}
	s.node.
		log.
		Debug().
		Str("self-id", s.node.PersistentState.SelfID.String()).
		Msg("closing node")

	s.node.PersistentState.mu.Lock()
	s.node.Closed = true
	s.node.PersistentState.mu.Unlock()

	err := s.cluster.Close()
	s.lock.Unlock()

	return err
}

// randomTimer returns tickers ranging from 150ms to 300ms.
func randomTimer(node *Node) *time.Timer {
	randomInt := rand.Intn(150) + 450
	node.log.
		Debug().
		Str("self-id", node.PersistentState.SelfID.String()).
		Int("random timer set to", randomInt).
		Msg("heart beat timer")
	return time.NewTimer(time.Duration(randomInt) * time.Millisecond)
}

// processIncomingData is responsible for parsing the incoming data and calling
// appropriate functions based on the request type.
func (s *SimpleServer) processIncomingData(data *incomingData) error {

	ctx := context.TODO()

	switch data.msg.Kind() {
	case message.KindRequestVoteRequest:
		requestVoteRequest := data.msg.(*message.RequestVoteRequest)
		requestVoteResponse := s.RequestVoteResponse(requestVoteRequest)
		payload, err := message.Marshal(requestVoteResponse)
		if err != nil {
			return err
		}

		err = data.conn.Send(ctx, payload)
		if err != nil {
			return err
		}
	case message.KindRequestVoteResponse:
		requestVoteResponse := data.msg.(*message.RequestVoteResponse)
		if requestVoteResponse.GetVoteGranted() {
			s.lock.Lock()
			voterID := s.getNodeID(data.conn)
			s.node.log.
				Debug().
				Str("received vote from", voterID.String()).
				Msg("voting from peer")
			selfID := s.node.PersistentState.SelfID
			s.lock.Unlock()
			votesRecieved := atomic.AddInt32(&s.node.VolatileState.Votes, 1)

			// Check whether this node has already voted.
			// Else it can vote for itself.
			s.node.PersistentState.mu.Lock()

			if s.node.PersistentState.VotedFor == nil {
				s.node.PersistentState.VotedFor = selfID
				s.node.log.
					Debug().
					Str("self-id", selfID.String()).
					Msg("node voting for itself")
				votesRecieved = atomic.AddInt32(&s.node.VolatileState.Votes, 1)
			}
			// Election win criteria, votes this node has is majority in the cluster and
			// this node is not already the Leader.
			if votesRecieved > int32(len(s.node.PersistentState.PeerIPs)/2) && s.node.State != StateLeader.String() {
				// This node has won the election.
				s.node.State = StateLeader.String()
				s.node.PersistentState.LeaderID = selfID
				s.node.log.
					Debug().
					Str("self-id", selfID.String()).
					Msg("node elected leader")
				// Reset the votes of this term once its elected leader.
				s.node.VolatileState.Votes = 0
				s.node.PersistentState.mu.Unlock()
				s.startLeader(selfID.String())
				return nil
			}
			s.node.PersistentState.mu.Unlock()
		}
	case message.KindAppendEntriesRequest:
		appendEntriesRequest := data.msg.(*message.AppendEntriesRequest)
		appendEntriesResponse := s.AppendEntriesResponse(appendEntriesRequest)
		payload, err := message.Marshal(appendEntriesResponse)
		if err != nil {
			return err
		}
		err = data.conn.Send(ctx, payload)
		if err != nil {
			log.Printf("error in sending AppendEntriesResponse: %v\n", err)
			return err
		}
	case message.KindAppendEntriesResponse:

		s.node.log.Debug().
			Str("node-id", s.getNodeID(data.conn).String()).
			Msg("received append entries response")

		appendEntriesResponse := data.msg.(*message.AppendEntriesResponse)

		s.node.PersistentState.mu.Lock()
		savedCurrentTerm := s.node.PersistentState.CurrentTerm
		selfID := s.node.PersistentState.SelfID.String()
		s.node.PersistentState.mu.Unlock()

		currNextIndex, offset := s.getNextIndex(data.conn)
		// If the term in the other node is greater than this node's term,
		// it means that this node is not up to date and has to step down
		// from being a leader.
		if appendEntriesResponse.Term > savedCurrentTerm {
			s.node.log.Debug().
				Str(selfID, "stale term").
				Str("following newer node", data.conn.RemoteID().String())
			s.node.becomeFollower(appendEntriesResponse.Term, s.getNodeID(data.conn))
			return nil
		}

		if s.node.State == StateLeader.String() && appendEntriesResponse.Term == savedCurrentTerm {
			if appendEntriesResponse.Success {
				s.node.PersistentState.mu.Lock()
				s.updateNextIndex(int(appendEntriesResponse.EntriesLength), offset, currNextIndex)
				s.node.PersistentState.mu.Unlock()
			} else {
				// If this appendEntries request failed,
				// proceed and retry in the next cycle.
				s.node.log.
					Debug().
					Str("self-id", selfID).
					Str("received failure to append entries from", data.conn.RemoteID().String()).
					Msg("failed to append entries")
			}
		}
	// When the leader gets a forwarded append input message from one of it's followers.
	case message.KindLogAppendRequest:
		// This log append request was meant to the leader ONLY.
		// This handles issues where the leader changed in the transit.
		if s.node.PersistentState.LeaderID != s.node.PersistentState.SelfID {
			s.relayDataToServer(data.msg.(*message.LogAppendRequest))
			return nil
		}
		logAppendRequest := data.msg.(*message.LogAppendRequest)
		input := logAppendRequest.Data
		logData := message.NewLogData(s.node.PersistentState.CurrentTerm, input)
		s.node.PersistentState.Log = append(s.node.PersistentState.Log, logData)
	}
	return nil
}

// relayDataToServer sends the input log from the follower to a leader node.
// TODO: Figure out what to do with the errors generated here.
func (s *SimpleServer) relayDataToServer(req *message.LogAppendRequest) {
	ctx := context.Background()

	payload, _ := message.Marshal(req)

	leaderNodeConn := s.cluster.Nodes()[s.node.PersistentState.ConnIDMap[s.node.PersistentState.LeaderID]]
	_ = leaderNodeConn.Send(ctx, payload)
}

// OnRequestVotes is a hook setter for RequestVotesRequest.
func (s *SimpleServer) OnRequestVotes(hook func(network.Conn)) {
	s.onRequestVotes = hook
}

// OnLeaderElected is a hook setter for LeadeElectedRequest.
func (s *SimpleServer) OnLeaderElected(hook func()) {
	s.onLeaderElected = hook
}

// OnAppendEntriesRequest is a hook setter for AppenEntriesRequest.
func (s *SimpleServer) OnAppendEntriesRequest(hook func(network.Conn)) {
	s.onAppendEntriesRequest = hook
}

// OnAppendEntriesResponse is a hook setter for AppenEntriesRequest.
func (s *SimpleServer) OnAppendEntriesResponse(hook func()) {
	s.onAppendEntriesResponse = hook
}

// OnCompleteOneRound is a hook setter for completion for one round of raft.
func (s *SimpleServer) OnCompleteOneRound(hook func()) {
	s.onCompleteOneRound = hook
}

// getNodeID finds the ID of the node from it's connection.
func (s *SimpleServer) getNodeID(conn network.Conn) id.ID {
	s.node.PersistentState.mu.Lock()
	defer s.node.PersistentState.mu.Unlock()

	for k, v := range s.node.PersistentState.ConnIDMap {
		if s.node.PersistentState.PeerIPs[v] == conn {
			return k
		}
	}

	return nil
}

// getNextIndex allows the leader to iterate through the available
// slice of connections of its peers and find the respective "nextIndex"
// value of the node which sent the AppendEntriesResponse. It returns
// the nextIndex value and the offset of the node for future use.
//
// A -1 int is returned on not finding the connection - which is not
// supposed to happen, EVER.
func (s *SimpleServer) getNextIndex(conn network.Conn) (int, int) {
	s.node.PersistentState.mu.Lock()
	defer s.node.PersistentState.mu.Unlock()

	for i := range s.node.PersistentState.PeerIPs {
		if conn == s.node.PersistentState.PeerIPs[i] {
			return s.node.VolatileStateLeader.NextIndex[i], i
		}
	}
	return -1, -1
}

func (s *SimpleServer) updateNextIndex(len, offset, currNextIndex int) {
	s.node.VolatileStateLeader.NextIndex[offset] = currNextIndex + len
}

func newIncomingData(conn network.Conn, msg message.Message) *incomingData {
	return &incomingData{
		conn,
		msg,
	}
}
