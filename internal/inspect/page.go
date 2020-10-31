package inspect

import (
	"fmt"
	"strconv"

	"github.com/xqueries/xdb/internal/engine/storage"
	"github.com/xqueries/xdb/internal/inspect/format"
)

// ProcessPagesCommand responds to a "Pages" query command.
// The response is a pre-formatted string which can be printed
// on the CLI.
func (i *Inspector) ProcessPagesCommand() (string, error) {

	return "", nil
}

// ProcessPagesCommand responds to a "Page" query command.
// This command moves the scope of the CLI into the scope
// of the page being queried, thus making the CLI stateful.
//
// The response is a pre-formatted string which can be printed
// on the CLI.
func (i *Inspector) ProcessPageCommand(args string) (string, error) {
	pageManager, err := storage.NewPageManager(i.file)
	if err != nil {
		return "", err
	}

	intID, err := strconv.Atoi(args)
	if err != nil {
		return "", err
	}

	page, err := pageManager.ReadPage(uint32(intID))
	if err != nil {
		return "", err
	}

	i.enterScope(NewScope("page", fmt.Sprint(page.ID())))

	return format.Page(page), nil
}
