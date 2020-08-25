package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xqueries/xdb/internal/engine"
	"github.com/xqueries/xdb/internal/engine/profile"
)

func TestExample01(t *testing.T) {
	RunAndCompare(t, Test{
		Name:      "example01",
		Statement: `VALUES (RANDOM())`,
		EngineOptions: []engine.Option{
			engine.WithRandomProvider(func() int64 { return 85734726843 }),
		},
	})
}

func TestExample02(t *testing.T) {
	timestamp, err := time.Parse(time.RFC3339, "2020-07-02T14:03:27Z")
	assert.NoError(t, err)

	RunAndCompare(t, Test{
		Name:      "example02",
		Statement: `VALUES (NOW(), RANDOM())`,
		EngineOptions: []engine.Option{
			engine.WithTimeProvider(func() time.Time { return timestamp }),
			engine.WithRandomProvider(func() int64 { return 85734726843 }),
		},
	})
}

func TestExample02WithProfile(t *testing.T) {
	timestamp, err := time.Parse(time.RFC3339, "2020-07-02T14:03:27Z")
	assert.NoError(t, err)

	p := profile.NewProfiler()

	RunAndCompare(t, Test{
		Name:      "example02",
		Statement: `VALUES (NOW(), RANDOM())`,
		EngineOptions: []engine.Option{
			engine.WithTimeProvider(func() time.Time { return timestamp }),
			engine.WithRandomProvider(func() int64 { return 85734726843 }),
			engine.WithProfiler(p),
		},
	})

	t.Logf("profile:\n%v", p.Profile().String())
}

func TestExample03(t *testing.T) {
	RunAndCompare(t, Test{
		Name:      "example03",
		Statement: `SELECT * FROM (VALUES (1, 2, 3), (4, 5, 6), (7, 5, 9))`,
	})
}

func TestExample04(t *testing.T) {
	RunAndCompare(t, Test{
		Name:      "example04",
		Statement: `SELECT * FROM (VALUES (1, 2, 3), (4, 5, 6), (7, 5, 9)) WHERE column2 = 5`,
	})
}

func TestExample05(t *testing.T) {
	RunAndCompare(t, Test{
		Name:      "example05",
		Statement: `SELECT column2 AS leftCol, column3 AS rightCol FROM (VALUES (1, 2, 3), (4, 3, 6), (7, 5, 9)) WHERE column2 >= 3`,
	})
}
