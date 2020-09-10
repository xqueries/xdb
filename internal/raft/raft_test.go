package raft

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/id"
	"github.com/xqueries/xdb/internal/network"
	networkmocks "github.com/xqueries/xdb/internal/network/mocks"
	"github.com/xqueries/xdb/internal/raft/cluster"
	"github.com/xqueries/xdb/internal/raft/message"
	raftmocks "github.com/xqueries/xdb/internal/raft/mocks"
)

// TestRaftFromLeaderPerspective tests the entire raft operation for one round of election
// and AppendEntries where the non-mocked node is the leader. Following is how it operates:
// 1. The test creates a mock cluster and assigns a clusterID to it.
// 2. Four mocked connections are created and ID's are assigned to those connections.
// 3. Send characteristic is set on the four connections to facilitate sending of
//    a RequestVoteRequest. It returns nil as no error is expected.
// 4. Receive characteristic is set on the four connections and they're set to respond
//	  with a response for the votes with a positive response.
// 5. Receive characteristic is set on the four connections again for a heartbeat response.
// 6. Cluster's "Nodes", "OwnID", "Receive" characteristics are set to appropriate responses.
// 7. The hooks are set in order to end the raft operation as soon as the append entries
// 	  requests are registered.
func TestRaftFromLeaderPerspective(t *testing.T) {
	t.SkipNow()
	assert := assert.New(t)
	ctx := context.Background()
	log := zerolog.New(os.Stdout).With().Logger().Level(zerolog.GlobalLevel())

	// Create a new cluster.
	cluster := new(raftmocks.Cluster)
	clusterID := id.Create()

	// Mock 4 other nodes in the cluster.
	conn1 := new(networkmocks.Conn)
	conn2 := new(networkmocks.Conn)
	conn3 := new(networkmocks.Conn)
	conn4 := new(networkmocks.Conn)

	connSlice := []network.Conn{
		conn1,
		conn2,
		conn3,
		conn4,
	}

	conn1 = addRemoteID(conn1)
	conn2 = addRemoteID(conn2)
	conn3 = addRemoteID(conn3)
	conn4 = addRemoteID(conn4)

	conn1.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn2.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn3.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn4.On("Send", ctx, mock.IsType([]byte{})).Return(nil)

	reqVRes1 := message.NewRequestVoteResponse(1, true)
	// payload1, err := message.Marshal(reqVRes1)
	// assert.NoError(err)

	cluster.On("Receive", ctx).Return(conn1, reqVRes1, nil).Once()
	cluster.On("Receive", ctx).Return(conn2, reqVRes1, nil).Once()
	cluster.On("Receive", ctx).Return(conn3, reqVRes1, nil).Once()
	cluster.On("Receive", ctx).Return(conn4, reqVRes1, nil).Once()

	appERes1 := message.NewAppendEntriesResponse(1, true)
	// payload2, err := message.Marshal(appERes1)
	// assert.NoError(err)

	cluster.On("Receive", ctx).Return(conn1, appERes1, nil)
	cluster.On("Receive", ctx).Return(conn2, appERes1, nil)
	cluster.On("Receive", ctx).Return(conn3, appERes1, nil)
	cluster.On("Receive", ctx).Return(conn4, appERes1, nil)

	// set up cluster to return the slice of connections on demand.
	cluster.
		On("Nodes").
		Return(connSlice)

	// return cluster ID
	cluster.
		On("OwnID").
		Return(clusterID)

	cluster.On("Close").Return(nil)

	server := newServer(
		log,
		cluster,
		timeoutProvider,
	)

	times := 0
	server.OnRequestVotes(func(msg *message.RequestVoteRequest) {})
	server.OnLeaderElected(func() {})
	server.OnAppendEntries(func() {
		times++
		if times == 5 {
			err := server.Close()
			if err != network.ErrClosed {
				assert.NoError(err)
			}
		}
	})
	err := server.Start(ctx)
	assert.NoError(err)
}

func TestRaftFromFollowerPerspective(t *testing.T) {
	t.SkipNow()
	assert := assert.New(t)
	ctx := context.Background()
	log := zerolog.New(os.Stdout).With().Logger().Level(zerolog.GlobalLevel())

	// Create a new cluster.
	cluster := new(raftmocks.Cluster)
	clusterID := id.Create()

	// Mock 4 other nodes in the cluster.
	conn1Leader := new(networkmocks.Conn)
	conn2 := new(networkmocks.Conn)
	conn3 := new(networkmocks.Conn)
	conn4 := new(networkmocks.Conn)

	connSlice := []network.Conn{
		conn1Leader,
		conn2,
		conn3,
		conn4,
	}

	conn1Leader = addRemoteID(conn1Leader)
	conn2 = addRemoteID(conn2)
	conn3 = addRemoteID(conn3)
	conn4 = addRemoteID(conn4)

	cluster.
		On("Nodes").
		Return(connSlice)

	cluster.
		On("OwnID").
		Return(clusterID)

	cluster.On("Close").Return(nil)

	server := newServer(
		log,
		cluster,
		timeoutProvider,
	)

	reqV := message.NewRequestVoteRequest(1, id.Create(), 1, 1)

	cluster.On("Receive", ctx).Return(conn1Leader, reqV, nil).Once()

	leaderID := id.Create()

	// Allows this node to send a request vote to all nodes.
	conn1Leader.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn2.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn3.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn4.On("Send", ctx, mock.IsType([]byte{})).Return(nil)

	appEnt := message.NewAppendEntriesRequest(3, leaderID, 1, 1, nil, 1)

	cluster.On("Receive", ctx).Return(conn1Leader, appEnt, nil)

	times := 0
	server.OnRequestVotes(func(msg *message.RequestVoteRequest) {})
	server.OnLeaderElected(func() {})
	server.OnAppendEntries(func() {
		times++
		if times == 5 {
			err := server.Close()
			if err != network.ErrClosed {
				assert.NoError(err)
			}
		}
	})

	err := server.Start(ctx)
	assert.NoError(err)
}

func addRemoteID(conn *networkmocks.Conn) *networkmocks.Conn {
	cID := id.Create()
	conn.On("RemoteID").Return(cID)
	return conn
}

func timeoutProvider(node *Node) *time.Timer {
	node.log.
		Debug().
		Str("self-id", node.PersistentState.SelfID.String()).
		Int("random timer set to", 150).
		Msg("heart beat timer")
	return time.NewTimer(time.Duration(150) * time.Millisecond)
}

func TestIntegration(t *testing.T) {
	log := zerolog.New(os.Stdout).With().Logger().Level(zerolog.GlobalLevel())

	assert := assert.New(t)
	operations := []OpData{
		{
			Op: SendData,
			Data: &OpSendData{
				Data: []*command.Command{},
			},
		},
		{
			Op:   StopNode,
			Data: &OpStopNode{},
		},
	}
	opParams := OperationParameters{
		Rounds:     2,
		TimeLimit:  2,
		Operations: operations,
	}

	testNetwork := cluster.NewTCPTestNetwork(t, 5)

	cfg := NetworkConfiguration{}
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)

	raftNodes := createRaftNodes(log, testNetwork)

	raftTest := NewSimpleRaftTest(log, opParams, cfg, raftNodes, cancelFunc)

	go func() {
		err := raftTest.BeginTest(ctx)
		assert.Nil(err)
	}()

	<-time.After(time.Duration(opParams.TimeLimit) * time.Second)
}
