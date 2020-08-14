package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
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

		if len(clusters) > 0 {
			c.Join(ctx, clusters[0].server.Addr().String())
		}
		clusters = append(clusters, c)
	}

	return &TCPTestNetwork{
		Clusters: clusters,
	}
}
