package cli

import (
	"fmt"
	"io"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/version"
)

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printHelp(stdout)
		return 0
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version.Current)
		return 0
	case "help", "-h", "--help":
		printHelp(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
		printHelp(stderr)
		return 2
	}
}

func printHelp(output io.Writer) {
	fmt.Fprintln(output, "agent-trace-reliability-control-plane")
	fmt.Fprintln(output, "usage: tracecontrol <command>")
	fmt.Fprintln(output, "commands: version, help")
}
