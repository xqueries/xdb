package raft

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/xqueries/xdb/internal/id"
	"github.com/xqueries/xdb/internal/network"
	networkmocks "github.com/xqueries/xdb/internal/network/mocks"
	raftmocks "github.com/xqueries/xdb/internal/raft/mocks"
	"os"
	"testing"
	"time"
)

// Test_StartElection tests the StartElection function whose sole
// responsibility is to eventually call Send on the connections
// that are in the SimpleServer's cluster, which is being tested here.
func Test_StartElection(t *testing.T) {
	ctx := context.Background()
	log := zerolog.New(os.Stdout).With().Logger().Level(zerolog.GlobalLevel())

	cluster := new(raftmocks.Cluster)
	clusterID := id.Create()

	conn1 := new(networkmocks.Conn)
	conn2 := new(networkmocks.Conn)

	connSlice := []network.Conn{
		conn1,
		conn2,
	}

	conn1 = addRemoteID(conn1)
	conn2 = addRemoteID(conn2)

	server := newServer(
		log,
		cluster,
		timeoutProvider,
	)

	cluster.On("Nodes").Return(connSlice)
	cluster.On("OwnID").Return(clusterID)

	conn1.On("Send", ctx, mock.IsType([]byte{})).Return(nil)
	conn2.On("Send", ctx, mock.IsType([]byte{})).Return(nil)

	node := NewRaftNode(cluster)
	server.node = node
	server.StartElection(ctx)

	time.Sleep(5* time.Second)
}