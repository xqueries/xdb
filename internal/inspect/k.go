package inspect

func (i *Inspector) ProcessKCommand() (string, error) {
	if i.CurrentScope == i.HomeScope {
		return "", ErrCantExitScope
	}
	i.CurrentScope = i.HomeScope
	return "exiting scope", nil
}
