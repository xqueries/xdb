package inspect

import (
	"strings"
)

const (
	Page     = "page"
	Table    = "table"
	Overview = "overview"
)

func inputParser(input string) CommandData {
	args := strings.Split(input, " ")
	switch args[0] {
	case Page:
		return NewCommandData(CommandPageDebug, args[1])
	case Table:
		return NewCommandData(CommandTableDebug, args[1])
	case Overview:
		return NewCommandData(CommandOverview, args[1])
	}
	return NewCommandData(CommandUnsupported, "")
}
