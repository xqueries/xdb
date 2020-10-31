package inspect

type scope struct {
	param string
	id    string
}

func NewScope(p string, id string) scope {
	return scope{
		param: p,
		id:    id,
	}
}

func (s scope) String() string {
	if s.id == "" {
		return s.param
	}
	return s.param + " " + s.id
}
