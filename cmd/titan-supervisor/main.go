package main

import (
	"os"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	lcli "github.com/Filecoin-Titan/titan/cli"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/Filecoin-Titan/titan/node/repo"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

const (
	FlagSupervisorRepo = "supervisor-repo"
)

func main() {
	types.RunningNodeType = types.NodeSupervisor
	titanlog.SetupLogLevels()

	local := []*cli.Command{
		lcli.SupervisorCmds,
	}

	app := &cli.App{
		Name:                 "titan-supervisor",
		Usage:                "Titan supervisor node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagSupervisorRepo,
				EnvVars: []string{"TITAN_SUPERVISOR_PATH"},
				Value:   "~/.titansupervisor",
				Usage:   "Specify supervisor repo path",
			},
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Supervisor

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}
}
