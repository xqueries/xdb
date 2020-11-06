package inspect

func (i *Inspector) ProcessKCommand() (string, error) {
	if i.CurrentScope == i.HomeScope {
		return "", ErrCantExitScope
	}

	if i.CurrentScope.param == "page" {
		i.PageData = nil
	}
	i.CurrentScope = i.HomeScope

	return "exiting scope", nil
}
