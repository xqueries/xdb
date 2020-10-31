package inspect

import (
	"os"

	"github.com/spf13/afero"

	"github.com/rs/zerolog"
	"github.com/xqueries/xdb/internal/engine/storage"

	"github.com/xqueries/xdb/internal/engine"
)

// Inspector aggregates all the types necessary for an inspection run.
type Inspector struct {
	e    engine.Engine
	file afero.File

	HomeScope    scope
	CurrentScope scope
	Delimiter    string
	Log          zerolog.Logger
}

// NewInspector returns a new instance of the Inspector.
func NewInspector(filePath string, log zerolog.Logger) *Inspector {
	// The file is opened in an O_RDONLY mode,
	// complying with the policies of the tool.
	file, err := os.Open(filePath)
	if err != nil {
		log.Err(err)
	}
	f, err := storage.Open(file)
	if err != nil {
		log.Err(err)
	}
	e, err := engine.New(f, engine.WithLogger(log))
	if err != nil {
		log.Err(err)
	}

	homeScope := NewScope("xdb inspect", "")
	currentScope := homeScope
	delimiter := ">"
	return &Inspector{
		e:            e,
		file:         file,
		HomeScope:    homeScope,
		CurrentScope: currentScope,
		Delimiter:    delimiter,
		Log:          log,
	}
}

// Inspect runs the inspection command provided as the argument.
// Currently, inspect is responsible for parsing the command and
// then processing it. The parsing will be moved out once we have
// a supporting "ishell" implementation.
func (i *Inspector) Inspect(input string) (string, error) {

	cmd, err := inputParser(input)
	if err != nil {
		return "", err
	}
	if cmd.Type == CommandUnsupported {
		return "", ErrUnsupportedCommand
	}

	res, err := i.ProcessCommand(cmd)
	if err != nil {
		return "", err
	}
	return res, nil
}

// enterScope enters the given scope for the Inspector.
func (i *Inspector) enterScope(scope scope) {
	i.CurrentScope = scope
}

// exitScope returns to the parent scope of the Inspector.
func (i *Inspector) exitScope() {
	i.CurrentScope = i.HomeScope
}

func (i *Inspector) GenerateLabel() string {
	return i.CurrentScope.String() + " " + i.Delimiter
}
