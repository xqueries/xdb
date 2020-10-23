package inspect

type CommandData struct {
	Type Command
	Args string
}

type Command string

const (
	CommandPageDebug   = Command("Page Debug")
	CommandTableDebug  = Command("Table Debug")
	CommandOverview    = Command("Overview")
	CommandUnsupported = Command("Unsupported command")
)

func (c Command) String() string { return string(c) }

func NewCommandData(c Command, args string) CommandData {
	return CommandData{
		Type: c,
		Args: args,
	}
}
