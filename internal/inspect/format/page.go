package format

import (
	"fmt"
	"strings"

	"github.com/xqueries/xdb/internal/engine/storage/page"
)

// Page formats a *page.Page to a human readable string.
func Page(page *page.Page) string {
	var sb strings.Builder
	sb = appendID(page, sb)
	sb = appendCellCount(page, sb)
	sb = appendDirtyStatus(page, sb)
	return sb.String()
}

func appendID(p *page.Page, sb strings.Builder) strings.Builder {
	sb.WriteString("ID: ")
	sb.WriteString(fmt.Sprint(p.ID()))
	sb.WriteString("\n")
	return sb
}

func appendCellCount(p *page.Page, sb strings.Builder) strings.Builder {
	sb.WriteString("Cells: ")
	sb.WriteString(fmt.Sprint(p.CellCount()))
	sb.WriteString("\n")
	return sb
}

func appendDirtyStatus(p *page.Page, sb strings.Builder) strings.Builder {
	sb.WriteString("Dirty: ")
	sb.WriteString(fmt.Sprint(p.Dirty()))
	sb.WriteString("\n")
	return sb
}
