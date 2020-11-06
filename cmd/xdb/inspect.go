package main

import (
	"fmt"

	"github.com/manifoldco/promptui"
	"github.com/xqueries/xdb/internal/inspect"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

// inspectXDB implements the xdb inspect command.
// This allows the user to inspect the ".xdb" file
// and query its contents explicitly.
//
// This is a READ ONLY command and no queries can be run
// through the inspector.
func inspectXDB(cmd *cobra.Command, args []string) {
	log := cmd.Context().Value(ctxKeyLog).(zerolog.Logger)

	i, err := inspect.NewInspector(args[0], log)
	if err != nil {
		i.Log.Error().Err(err).Msg("error in starting inspector")
	}
	validate := func(input string) error {
		return nil
	}

	prompt := promptui.Prompt{
		Label:    i.GenerateLabel(),
		Validate: validate,
	}

	beginning := true
	for {
		if beginning {
			fmt.Println(inspect.WelcomeMessage)
			beginning = false
		}

		response, err := prompt.Run()
		if err != nil || response == "q" || response == "Q" {
			fmt.Println(inspect.ExitMessage)
			return
		}

		if response != "" {
			// The command that was input to the CLI is
			// passed to the inspect package. The returned
			// data/string is formatted and only needs to be
			// printed straightaway.
			// Even errors are propagated as strings here,
			// which can be easily displayed
			result, err := i.Inspect(response)
			if err != nil {
				i.Log.Error().Err(err).Msg("error in CLI, stopping")
			} else {
				fmt.Println(result)
				// Update the label if the scope is changed.
				prompt.Label = i.GenerateLabel()
			}
		}

	}
}
