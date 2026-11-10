package chassis

import (
	"flag"
	"fmt"
	"io"
)

const (
	ExitSuccess    = 0
	ExitUsage      = 2
	ExitConfig     = 78
	ExitDependency = 69
	ExitRuntime    = 70
)

type BootContext struct {
	Command      string
	ConfigPath   string
	ValidateOnly bool
}

func Preflight(args []string, _ []string, stdout io.Writer) (*BootContext, int, error) {
	fs := flag.NewFlagSet("service", flag.ContinueOnError)
	fs.SetOutput(stdout)

	var configPath string
	var validateOnly bool

	fs.StringVar(&configPath, "config", "config/default.yaml", "path to configuration file")
	fs.BoolVar(&validateOnly, "validate-only", false, "validate configuration and desired state without starting")

	if err := fs.Parse(args); err != nil {
		return nil, ExitUsage, fmt.Errorf("argument parse error: %w", err)
	}

	cmd := "serve"
	if len(fs.Args()) > 0 {
		cmd = fs.Args()[0]
	}

	return &BootContext{
		Command:      cmd,
		ConfigPath:   configPath,
		ValidateOnly: validateOnly,
	}, ExitSuccess, nil
}
