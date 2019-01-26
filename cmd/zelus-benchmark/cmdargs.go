package main

import (
	"flag"
	"io"
)

type cmdArgs struct {
	fs       *flag.FlagSet
	help     bool
	Hostname string
	Socket   string
	Clients  uint
	Requests uint
	Multi    uint
	Datasize uint
	Prefix   string
	Tests    string
	Loop     bool
}

func newCmdArgs(output io.Writer) (ca *cmdArgs) {
	ca = &cmdArgs{
		fs: flag.NewFlagSet("zelus-benchmark", flag.ContinueOnError),
	}
	ca.fs.SetOutput(output)
	ca.fs.BoolVar(&ca.help, "-help", false, "Shows usage")
	ca.fs.StringVar(&ca.Hostname, "h", "localhost:1234", "Server hostname")
	ca.fs.StringVar(&ca.Socket, "s", "", "Server socket (overrides hostname)")
	ca.fs.UintVar(&ca.Clients, "c", 50, "Number of parallel connections")
	ca.fs.UintVar(&ca.Requests, "n", 100000, "Total number of requests")
	ca.fs.UintVar(&ca.Multi, "m", 1, "Number of requests per command")
	ca.fs.UintVar(&ca.Datasize, "d", 4096, "Data size of value in bytes")
	ca.fs.StringVar(&ca.Prefix, "p", "zelus-benchmark_", "Prefix of benchmark keys")
	ca.fs.StringVar(&ca.Tests, "t", "set,get", "Comma separated list of tests")
	ca.fs.BoolVar(&ca.Loop, "l", false, "Loop. Run the tests forever")
	return
}

func (ca *cmdArgs) Parse(arguments []string) (err error) {
	err = ca.fs.Parse(arguments)
	return
}
