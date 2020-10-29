package inspect

// Help data for the user.
const (
	WelcomeMessage = `Welcome to xdb inspect.
Type "help" to get list of available commands or "q" to quit the terminal.`

	HelpMain = `usage: command args --flags

These are the supported commands:
	page   Get details about a particular page, type page help for more.
	table  Get details about a particular table, type table help for more.`

	HelpPages = `
	NAME
		Pages - obtain the list of existing pages with names/numbers.

	SYNOPSIS
		Pages provides an exhaustive list of the pages used in the xdb file.

	USAGE
		homeScope> pages
		<-- Details about the pages in xdb -->`

	HelpPage = `
	NAME
		Page - obtain page related information on inspecting.
	
	SYNOPSIS
		Page starts an interactive terminal in the scope of this page.

	USAGE
		homeScope> page 10
		<-- displays relevant info -->
		<-- enters page 10 scope -->

		page 10> keyForCell
		<-- displays value for related key -->

		page 10> k
		<-- exits page scope -->

		homeScope>
	OPTIONS
		--table
			Display pages only with table headers.`
	HelpTable = `
	NAME
		Table`

	ExitMessage = "Exiting xdb inspect, seeya."
)
