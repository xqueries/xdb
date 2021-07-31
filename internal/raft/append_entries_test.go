package raft

import (
	"github.com/xqueries/xdb/internal/id"
	networkmocks "github.com/xqueries/xdb/internal/network/mocks"
	raftmocks "github.com/xqueries/xdb/internal/raft/mocks"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/xqueries/xdb/internal/network"
	"github.com/xqueries/xdb/internal/raft/message"
)

// Test_FailureAppendEntriesResponse1 tests for failure of append entries
// due to leader term being lesser than the term of the operating node i.e
// the node who was requested the append entries.
func Test_FailureAppendEntriesResponse1(t *testing.T) {

	node,cluster,log := prepareBaseSystem()
	node.PersistentState.CurrentTerm = 3
	node.PersistentState.Log = []*message.LogData{
		{Term: 1, Entry: nil},
		{Term: 1, Entry: nil},
	}
	server := SimpleServer{
		node:            node,
		log:             log,
		cluster:         cluster,
		timeoutProvider: timeoutProvider,

	}

	msg := message.NewAppendEntriesRequest(2,id.Create(),1,1,nil,1)
	res := server.AppendEntriesResponse(msg)
	assert.Equal(t, int32(3),res.GetTerm())
	assert.False(t, res.GetSuccess())
	assert.Equal(t, int32(0), res.GetEntriesLength())
}

// Test_FailureAppendEntriesResponse2 tests for failure of append entries
// due to the committed logs not having the same term.
func Test_FailureAppendEntriesResponse2(t *testing.T) {
	node, cluster, log := prepareBaseSystem()

	node.PersistentState.CurrentTerm = 1
	node.PersistentState.Log = []*message.LogData{
		{Term: 1, Entry: nil},
		{Term: 1, Entry: nil},
	}
	node.VolatileState.CommitIndex = 2
	server := SimpleServer{
		node:            node,
		log:             log,
		cluster:         cluster,
		timeoutProvider: timeoutProvider,

	}

	msg := message.NewAppendEntriesRequest(3,id.Create(),1,1,nil,1)
	res := server.AppendEntriesResponse(msg)
	assert.Equal(t, int32(1),res.GetTerm())
	assert.False(t, res.GetSuccess())
	assert.Equal(t, int32(0), res.GetEntriesLength())
}

// Test_FailureAppendEntriesResponse3 tests for failures of append
// entries due to mismatch of the terms of the last committed logs
// in both interacting machines.
func Test_FailureAppendEntriesResponse3(t *testing.T) {
	node, cluster, log := prepareBaseSystem()
	node.PersistentState.CurrentTerm = 3
	node.PersistentState.Log = []*message.LogData{
		{Term: 1, Entry: nil},
		{Term: 2, Entry: nil},
	}
	node.VolatileState.CommitIndex = -1
	server := SimpleServer{
		node:            node,
		log:             log,
		cluster:         cluster,
		timeoutProvider: timeoutProvider,

	}

	msg := message.NewAppendEntriesRequest(3,id.Create(),1,1,nil,1)
	res := server.AppendEntriesResponse(msg)
	assert.Equal(t, int32(3),res.GetTerm())
	assert.False(t, res.GetSuccess())
	assert.Equal(t, int32(0), res.GetEntriesLength())
}

// Test_PassAppendEntries1 tests append entries where it returns
// a success status. This test doesn't append any entries to the
// approaching node's logs and can be considered a heartbeat type
// method call.
func Test_PassAppendEntries1(t *testing.T) {
	node, cluster, log := prepareBaseSystem()

	node.PersistentState.CurrentTerm = 3
	node.PersistentState.Log = []*message.LogData{
		{Term: 1, Entry: nil},
		{Term: 1, Entry: nil},
	}
	node.VolatileState.CommitIndex = -1
	server := SimpleServer{
		node:            node,
		log:             log,
		cluster:         cluster,
		timeoutProvider: timeoutProvider,

	}

	msg := message.NewAppendEntriesRequest(3,id.Create(),1,1,nil,1)
	res := server.AppendEntriesResponse(msg)
	assert.Equal(t, int32(3),res.GetTerm())
	assert.True(t, res.GetSuccess())
	assert.Equal(t, int32(0), res.GetEntriesLength())
}

// Test_PassAppendEntries2 tests a successful append entries call
// with a couple of entries to be added to the approaching node's
// logs. This tests whether the logs were appended and the acknowledgement
// of the function for the number of entries appended.
func Test_PassAppendEntries2(t *testing.T) {
	node, cluster, log := prepareBaseSystem()

	node.PersistentState.CurrentTerm = 3
	node.PersistentState.Log = []*message.LogData{
		{Term: 1, Entry: nil},
		{Term: 1, Entry: nil},
	}
	node.VolatileState.CommitIndex = -1
	server := SimpleServer{
		node:            node,
		log:             log,
		cluster:         cluster,
		timeoutProvider: timeoutProvider,

	}

	msg := message.NewAppendEntriesRequest(3,
		id.Create(),
		1,
		1,
		[]*message.LogData{
			{Term: 2, Entry: nil},
			{Term: 2, Entry: nil},
		},
		1)

	assert.Equal(t, 2, len(server.node.PersistentState.Log))

	res := server.AppendEntriesResponse(msg)
	assert.Equal(t, int32(3),res.GetTerm())
	assert.True(t, res.GetSuccess())
	assert.Equal(t, int32(2), res.GetEntriesLength())

	assert.Equal(t,4, len(server.node.PersistentState.Log))
}

// Test_PassAppendEntries3 tests removing of conflicting logs in
// the node's logs and then updating them up to the leader's logs.
func Test_PassAppendEntries3(t *testing.T) {
	node, cluster, log := prepareBaseSystem()

	node.PersistentState.CurrentTerm = 3
	node.PersistentState.Log = []*message.LogData{
		{Term: 1, Entry: nil},
		{Term: 1, Entry: nil},
		{Term: 3, Entry: nil},
		{Term: 3, Entry: nil},
	}
	node.VolatileState.CommitIndex = 1
	server := SimpleServer{
		node:            node,
		log:             log,
		cluster:         cluster,
		timeoutProvider: timeoutProvider,

	}

	msg := message.NewAppendEntriesRequest(5,
		id.Create(),
		1,
		1,
		[]*message.LogData{
			{Term: 4, Entry: nil},
			{Term: 4, Entry: nil},
		},
		1)

	// Pre check on the log length.
	assert.Equal(t, 4, len(server.node.PersistentState.Log))

	res := server.AppendEntriesResponse(msg)
	// TODO: Check term is 3, I suspect it should be changed to 4.
	assert.Equal(t, int32(3),res.GetTerm())
	assert.True(t, res.GetSuccess())
	assert.Equal(t, int32(2), res.GetEntriesLength())
	// Newly appended logs must have removed conflicting logs.
	assert.Equal(t,4, len(server.node.PersistentState.Log))
	// New entry's term.
	assert.Equal(t, int32(4),server.node.PersistentState.Log[2].GetTerm())
}

// prepareBaseSystem prepares the basis of all the tests on which Append Entries
// functionality is being tested.
// This involves creating a mock cluster with 3 nodes, which is created by adding
// 2 mocked connections to the mock cluster.
func prepareBaseSystem() (*Node,*raftmocks.Cluster,zerolog.Logger){
	log := zerolog.New(os.Stdout).With().Logger().Level(zerolog.GlobalLevel())

	cluster := new(raftmocks.Cluster)
	clusterID := id.Create()

	conn1 := new(networkmocks.Conn)
	conn2 := new(networkmocks.Conn)

	conn1 = addRemoteID(conn1)
	conn2 = addRemoteID(conn2)

	connSlice := []network.Conn{
		conn1,
		conn2,
	}

	cluster.On("Nodes").Return(connSlice)
	cluster.On("OwnID").Return(clusterID)

	node := NewRaftNode(cluster)

	return node, cluster, log
}