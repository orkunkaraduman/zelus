package main

import (
	"flag"
	"io"
)

type cmdArgs struct {
	fs       *flag.FlagSet
	help     bool
	Bind     string
	Socket   string
	Capacity uint
}

func newCmdArgs(output io.Writer) (ca *cmdArgs) {
	ca = &cmdArgs{
		fs: flag.NewFlagSet("zelus-server", flag.ContinueOnError),
	}
	ca.fs.SetOutput(output)
	ca.fs.BoolVar(&ca.help, "-help", false, "Shows usage")
	ca.fs.StringVar(&ca.Bind, "b", "localhost:1234", "Bind interface")
	ca.fs.StringVar(&ca.Socket, "s", "", "Socket (overrides bind)")
	ca.fs.UintVar(&ca.Capacity, "c", 100, "Capacity in MiB")
	return
}

func (ca *cmdArgs) Parse(arguments []string) (err error) {
	err = ca.fs.Parse(arguments)
	return
}
