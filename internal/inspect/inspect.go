package inspect

import "fmt"

func Inspect(cmd string) string {
	fmt.Printf("You wrote: %s\n", cmd)
	return "Nice work"
}
