package raft

import (
	"context"
	"io"

	"github.com/xqueries/xdb/internal/id"
	"github.com/xqueries/xdb/internal/network"
	"github.com/xqueries/xdb/internal/raft/message"
)

//go:generate mockery -case=snake -name=Cluster

// Cluster is a description of a cluster of servers.
type Cluster interface {
	OwnID() id.ID
	Nodes() []network.Conn
	Receive(context.Context) (network.Conn, message.Message, error)
	Broadcast(context.Context, message.Message) error
	io.Closer
}
