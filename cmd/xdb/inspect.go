package main

import (
	"fmt"
	"os"

	"github.com/xqueries/xdb/internal/inspect"

	"github.com/c-bata/go-prompt"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/xqueries/xdb/internal/engine"
	"github.com/xqueries/xdb/internal/engine/storage"
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func inspectXDB(cmd *cobra.Command, args []string) {
	log := cmd.Context().Value(ctxKeyLog).(zerolog.Logger)

	// The file is opened in an O_RDONLY mode,
	// complying with the policies of the tool.
	file, err := os.Open(args[0])
	if err != nil {
		log.Err(err)
	}
	f, err := storage.Open(file)
	if err != nil {
		log.Err(err)
	}
	_, err = engine.New(f, engine.WithLogger(log))
	if err != nil {
		log.Err(err)
	}

	beginning := true
	for {
		if beginning {
			fmt.Println("Welcome to xdb inspect.\nType \"help\" to get list of available commands.\n")
			beginning = false
		}
		t := prompt.Input("xdb inspect> ", completer)
		if t == "q" || t == "Q" {
			fmt.Println("Exiting xdb inspect, seeya.\n")
			return
		}
		if t != "" {
			// The command that was input to the CLI is
			// passed to the inspect package. The returned
			// data/string is formatted and only needs to be
			// printed straightaway.
			res := inspect.Inspect(t)
			fmt.Println(res)
		}
	}
}
