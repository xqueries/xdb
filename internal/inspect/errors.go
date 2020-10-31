package inspect

// Error is a helper type for creating constant errors.
type Error string

func (e Error) Error() string { return string(e) }

// Errors expected.
const (
	ErrUnsupportedCommand Error = "We dont support that command, type \"help\" to know the list of supported commands."
	ErrInCommandExecution Error = "Error in command execution, please type \"help commandName\" to understand usage"
	ErrInsufficientArgs   Error = "Insufficient arguments in the command, please type \" help commandName\" to understand usage"
	ErrExcessArgs         Error = "Excess arguments in the command, please type \" help commandName\" to understand usage"
	ErrCantExitScope      Error = "Already in home scope, can't exit."
)
