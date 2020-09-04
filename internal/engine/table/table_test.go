package table

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestTableSuite(t *testing.T) {
	suite.Run(t, new(TableSuite))
}

type TableSuite struct {
	suite.Suite
}
