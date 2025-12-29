package main

import (
	"fmt"
	"os"

	"github.com/RobertIndie/poc/fsmq/internal/perf"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]
	os.Args = os.Args[1:]

	switch subcommand {
	case "produce":
		perf.RunProduce()
	case "consume":
		perf.RunConsume()
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s <command> [flags]

Commands:
  produce    Run produce performance test
  consume    Run consume performance test

Use "%s <command> -h" for command-specific flags.
`, os.Args[0], os.Args[0])
}
