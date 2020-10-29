package inspect

import (
	"fmt"
)

// ProcessPagesCommand responds to a "Pages" query command.
// The response is a pre-formatted string which can be printed
// on the CLI.
func (i *Inspector) ProcessPagesCommand() (string, error) {
	fmt.Println("processing pages command")
	return "", nil
}

// ProcessPagesCommand responds to a "Page" query command.
// This command moves the scope of the CLI into the scope
// of the page being queried, thus making the CLI stateful.
//
// The response is a pre-formatted string which can be printed
// on the CLI.
func (i *Inspector) ProcessPageCommand(args string) (string, error) {
	fmt.Println("processing page command")
	fmt.Println(args)
	return "", nil
}
