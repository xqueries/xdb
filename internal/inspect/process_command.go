package inspect

import (
	"github.com/xqueries/xdb/internal/engine"
)

func processCommand(e engine.Engine, c CommandData) (string, error) {
	var (
		res string
		err error
	)
	switch c.Type {
	case CommandPageDebug:
		res, err = processPageCommand(e, c.Args)
	case CommandTableDebug:
	case CommandOverview:
	}
	return res, err
}
