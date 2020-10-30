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

func TestExample06(t *testing.T) {
	p := profile.NewProfiler()

	RunAndCompare(t, Test{
		Name:      "example06",
		Statement: `CREATE TABLE myTbl (id INTEGER, foo TEXT)`,
		EngineOptions: []engine.Option{
			engine.WithProfiler(p),
		},
	})

	t.Logf("profile:\n%v", p.Profile().String())
}

func TestExample07(t *testing.T) {
	t.Skip("see issue #70")
	p := profile.NewProfiler()

	RunAndCompare(t, Test{
		Name:      "example07",
		Statement: `SELECT column1 a, column2 b, column3 c FROM (VALUES (1, 9, 3), (3, 2, 19), (4, 5, 6), (3, 6, 1)) WHERE b >= 2*3-1`,
		EngineOptions: []engine.Option{
			engine.WithProfiler(p),
		},
	})

	t.Logf("profile:\n%v", p.Profile().String())
}

func TestExample08(t *testing.T) {
	p := profile.NewProfiler()

	RunAndCompare(t, Test{
		Name:       "example08",
		DBFileName: "trivial.xdb",
		Statement:  `SELECT col2 FROM table1`,
		EngineOptions: []engine.Option{
			engine.WithProfiler(p),
		},
	})

	t.Logf("profile:\n%v", p.Profile().String())
}
