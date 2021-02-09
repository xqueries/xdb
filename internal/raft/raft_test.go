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
// 8. The mechanism of a response to recieve is set only such that a RequestVote is asked for,
// 	  the cluster.Receive function responsds. This is done by listening on a closing channel,
//	  where the channel is closed if the RequestVote or the AppendEntries is recevied.
func TestRaftFromLeaderPerspective(t *testing.T) {
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

	server := newServer(
		log,
		cluster,
		timeoutProvider,
	)

	var (
		chanConn1 = make(chan time.Time)
		chanConn2 = make(chan time.Time)
		chanConn3 = make(chan time.Time)
		chanConn4 = make(chan time.Time)
	)

	server.OnRequestVotes(func(conn network.Conn) {
		switch conn {
		case conn1:
			close(chanConn1)
		case conn2:
			close(chanConn2)
		case conn3:
			close(chanConn3)
		case conn4:
			close(chanConn4)
		}
	})

	server.OnLeaderElected(func() {})

	var (
		chanConnAppE1 = make(chan time.Time)
		chanConnAppE2 = make(chan time.Time)
		chanConnAppE3 = make(chan time.Time)
		chanConnAppE4 = make(chan time.Time)
	)

	server.OnAppendEntriesRequest(func(conn network.Conn) {
		switch conn {
		case conn1:
			close(chanConnAppE1)
		case conn2:
			close(chanConnAppE2)
		case conn3:
			close(chanConnAppE3)
		case conn4:
			close(chanConnAppE4)
		}
		err := server.Close()
		if err != network.ErrClosed {
			assert.NoError(err)
		}
	})

	// set up cluster to return the slice of connections on demand.
	cluster.
		On("Nodes").
		Return(connSlice)

	// return cluster ID
	cluster.
		On("OwnID").
		Return(clusterID)

	cluster.On("Close").Return(nil)

	conn1.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn2.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn3.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn4.On("Send", ctx, mock.IsType([]byte{})).Return(nil)

	reqVRes1 := message.NewRequestVoteResponse(1, true)

	cluster.On("Receive", ctx).Return(conn1, reqVRes1, nil).WaitUntil(chanConn1).Once()
	cluster.On("Receive", ctx).Return(conn2, reqVRes1, nil).WaitUntil(chanConn2).Once()
	cluster.On("Receive", ctx).Return(conn3, reqVRes1, nil).WaitUntil(chanConn3).Once()
	cluster.On("Receive", ctx).Return(conn4, reqVRes1, nil).WaitUntil(chanConn4).Once()

	appERes1 := message.NewAppendEntriesResponse(1, true, 1)

	cluster.On("Receive", ctx).Return(conn1, appERes1, nil).WaitUntil(chanConnAppE1)
	cluster.On("Receive", ctx).Return(conn2, appERes1, nil).WaitUntil(chanConnAppE2)
	cluster.On("Receive", ctx).Return(conn3, appERes1, nil).WaitUntil(chanConnAppE3)
	cluster.On("Receive", ctx).Return(conn4, appERes1, nil).WaitUntil(chanConnAppE4)

	err := server.Start(ctx)
	assert.NoError(err)

}

func TestRaftFromFollowerPerspective(t *testing.T) {
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

	server.OnRequestVotes(func(network.Conn) {})

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

	server.OnLeaderElected(func() {})
	server.OnAppendEntriesResponse(func() {
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
			Op: StopNode,
			Data: &OpStopNode{
				3,
			},
		},
		{
			Op: StopNode,
			Data: &OpStopNode{
				4,
			},
		},
		//{
		//	Op: RestartNode,
		//	Data: &OpRestartNode{
		//		3,
		//	},
		//},
	}
	opParams := OperationParameters{
		Rounds:             10,
		TimeLimit:          5,
		Operations:         operations,
		OperationPushDelay: 500,
	}

	testNetwork := cluster.NewTCPTestNetwork(t, 5)

	cfg := NetworkConfiguration{}
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)

	raftNodes := createRaftNodes(log, testNetwork)

	raftTest := NewSimpleRaftTest(log, opParams, cfg, raftNodes, cancelFunc)

	err := raftTest.BeginTest(ctx)
	assert.Nil(err)
}
