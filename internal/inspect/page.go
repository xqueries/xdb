package inspect

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine"
)

func processPageCommand(e engine.Engine, args string) (string, error) {
	fmt.Println("processing page command at")
	fmt.Println(args)
	return "", nil
}
