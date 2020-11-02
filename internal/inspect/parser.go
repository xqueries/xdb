package inspect

import (
	"strings"
)

// Inputs from the user.
const (
	Pages    = "pages"
	Page     = "page"
	Tables   = "tables"
	Table    = "table"
	Overview = "overview"
	Help     = "help"
	K        = "k"
)

// inputParser is a temporary parser that will be removed when
// the "ishell" will be implemented with a command and args parser.
//
// If the command is not recognised, a "CommandUnsupported" command
// is returned. If there are errors such as excess or no arguments,
// a respective error is returned.
func inputParser(input string) (CommandData, error) {
	args := strings.Split(input, " ")
	switch args[0] {
	case Pages:
		// Pages doesn't accept any arguments.
		if len(args) > 1 {
			return CommandData{}, ErrExcessArgs
		}
		return NewCommandData(CommandPages, ""), nil
	case Page:
		// Page takes exactly one argument with
		// the command,
		if len(args) == 1 {
			return CommandData{}, ErrInsufficientArgs
		}
		return NewCommandData(CommandPage, args[1]), nil
	case Tables:
		// Tables doesn't accept any arguments.
		if len(args) > 1 {
			return CommandData{}, ErrExcessArgs
		}
		return NewCommandData(CommandTables, ""), nil
	case Table:
		// Table takes exactly one argument with
		// the command,
		if len(args) == 1 {
			return CommandData{}, ErrInsufficientArgs
		}
		return NewCommandData(CommandTable, args[1]), nil
	case Overview:
		// Overview doesn't need any arguments.
		if len(args) > 1 {
			return CommandData{}, ErrExcessArgs
		}
		return NewCommandData(CommandOverview, ""), nil
	case Help:
		// Help can't have more than 1 argument after the
		// actual "help" command.
		var argument string
		if len(args) > 2 {
			return CommandData{}, ErrExcessArgs
		}

		if len(args) != 1 {
			argument = args[1]
		}

		return NewCommandData(CommandHelp, argument), nil
	case K:
		// K doesn't need any arguments.
		if len(args) > 1 {
			return CommandData{}, ErrExcessArgs
		}
		return NewCommandData(CommandK, ""), nil
	}
	return NewCommandData(CommandUnsupported, ""), nil
}
