package inspect

func (i *Inspector) ProcessHelpCommand(arg string) (string, error) {
	if arg == "" {
		return HelpMain, nil
	} else {
		switch arg {
		case Pages:
			return HelpPages, nil
		case Page:
			return HelpPage, nil
		case Table:
			return HelpTable, nil
		}
	}
	return "", ErrUnsupportedCommand
}
