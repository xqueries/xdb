package inspect

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine"
)

type Inspector interface {
	Inspect(engine.Engine, string) string
}

func Inspect(e engine.Engine, input string) string {

	fmt.Printf("You wrote: %s\n", input)
	cmd := inputParser(input)
	if cmd.Type == CommandUnsupported {
		return "We dont support - type help whatever"
	}

	res, err := processCommand(e, cmd)
	if err != nil {

	}
	return res
}
