package inspect

import (
	"strings"
)

// Inputs from the user.
const (
	Pages    = "pages"
	Page     = "page"
	Table    = "table"
	Overview = "overview"
	Help     = "help"
)

// inputParser is a temporary parser that will be removed when
// the "ishell" will be implemented with a command and args parser.
func inputParser(input string) (CommandData, error) {
	args := strings.Split(input, " ")
	switch args[0] {
	case Pages:
		if len(args) > 1 {
			return CommandData{}, ErrExcessArgs
		}
		return NewCommandData(CommandPages, ""), nil
	case Page:
		return NewCommandData(CommandPage, args[1]), nil
	case Table:
		return NewCommandData(CommandTable, args[1]), nil
	case Overview:
		return NewCommandData(CommandOverview, args[1]), nil
	case Help:
		var argument string
		if len(args) != 1 {
			argument = args[1]
		}
		return NewCommandData(CommandHelp, argument), nil
	}
	return NewCommandData(CommandUnsupported, ""), nil
}
