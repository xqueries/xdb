package inspect

import (
	"fmt"
	"strconv"

	"github.com/xqueries/xdb/internal/engine/storage/page"

	"github.com/xqueries/xdb/internal/inspect/format"
)

// ProcessPagesCommand responds to a "Pages" query command.
// The response is a pre-formatted string which can be printed
// on the CLI.
func (i *Inspector) ProcessPagesCommand() (string, error) {
	ids := i.pgMgr.AllPageIDs()
	return fmt.Sprint(ids), nil
}

// ProcessPagesCommand responds to a "Page" query command.
// This command moves the scope of the CLI into the scope
// of the page being queried, thus making the CLI stateful.
//
// The response is a pre-formatted string which can be printed
// on the CLI.
func (i *Inspector) ProcessPageCommand(args string) (string, error) {

	page, err := i.getPageFromID(args)
	if err != nil {
		return "", err
	}

	i.enterScope(NewScope("page", fmt.Sprint(page.ID())))
	i.PageData = NewPageData(nil)

	return format.Page(page), nil
}

func (i *Inspector) ProcessCellsCommand(scope string) (string, error) {
	page, err := i.getPageFromID(scope)
	if err != nil {
		return "", err
	}

	slots := page.OccupiedSlots()
	i.PageData.slots = slots

	return format.Cells(slots), nil
}

func (i *Inspector) ProcessPageQueryCommand(scope string, args string) (string, error) {
	page, err := i.getPageFromID(scope)
	if err != nil {
		return "", err
	}
	intArg, _ := strconv.Atoi(args)
	cell := page.CellAt(i.PageData.slots[intArg])

	return fmt.Sprint(cell), nil
}

func (i *Inspector) getPageFromID(arg string) (*page.Page, error) {
	intID, err := strconv.Atoi(arg)
	if err != nil {
		return nil, err
	}

	return i.pgMgr.ReadPage(uint32(intID))
}
