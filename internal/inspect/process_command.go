package inspect

// ProcessCommand can be invoked on any command input from the
// user post parsing. This returns a a pre-formatted string that
// can be printed on the CLI, which will be based on the command
// executed.
func (i *Inspector) ProcessCommand(c CommandData) (string, error) {
	var (
		res string
		err error
	)
	if i.CurrentScope == i.HomeScope {
		switch c.Type {
		case CommandPages:
			res, err = i.ProcessPagesCommand()
		case CommandPage:
			res, err = i.ProcessPageCommand(c.Args)
		case CommandTables:
			res, err = i.ProcessTablesCommand()
		case CommandTable:
			res, err = i.ProcessTableCommand(c.Args)
		case CommandOverview:
		case CommandHelp:
			res, err = i.ProcessHelpCommand(c.Args)
		case CommandK:
			res, err = i.ProcessKCommand()
		default:
			res, err = "", ErrUnsupportedCommand
		}
	} else {
		scope := i.CurrentScope.id
		switch c.Type {
		case CommandCells:
			res, err = i.ProcessCellsCommand(scope)
		case CommandKeyQuery:
			res, err = i.ProcessPageQueryCommand(scope, c.Args)
		}
	}
	return res, err
}
