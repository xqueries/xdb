package inspect

// CommandData describes data describing a runnable Command.
type CommandData struct {
	Type Command
	Args string
}

// Command is a runnable command type.
type Command string

// Command list.
const (
	CommandPages       = Command("Pages")
	CommandPage        = Command("Page")
	CommandTable       = Command("Table")
	CommandOverview    = Command("Overview")
	CommandUnsupported = Command("Unsupported command")
	CommandHelp        = Command("help")
	CommandK           = Command("k")
)

func (c Command) String() string { return string(c) }

// NewCommandData returns an instance of CommandData with
// the provided arguments.
func NewCommandData(c Command, args string) CommandData {
	return CommandData{
		Type: c,
		Args: args,
	}
}
