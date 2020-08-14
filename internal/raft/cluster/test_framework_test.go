package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/xqueries/xdb/internal/network"
)

// TestNetwork encompasses the entire network on which
// the tests will be performed.
type TCPTestNetwork struct {
	Clusters []*tcpCluster
}

func NewTCPTestNetwork(t *testing.T, num int) *TCPTestNetwork {
	assert := assert.New(t)

	var clusters []*tcpCluster

	for i := 0; i < num; i++ {
		ctx := context.TODO()

		c := newTCPCluster(zerolog.Nop())
		assert.NoError(c.Open(ctx, ":0"))
		select {
		case <-c.server.Listening():
		case <-time.After(1 * time.Second):
			assert.FailNow("timeout")
		}

		for _, otherCluster := range clusters {
			conn, err := network.DialTCP(ctx, c.OwnID(), otherCluster.server.Addr().String())
			assert.NoError(err)
			c.AddConnection(conn)
		}
		clusters = append(clusters, c)
	}

	return &TCPTestNetwork{
		Clusters: clusters,
	}
}
