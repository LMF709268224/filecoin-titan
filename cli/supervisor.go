package cli

import (
	"strings"

	"github.com/Filecoin-Titan/titan/node/supervisor"
	"github.com/urfave/cli/v2"
)

var SupervisorCmds = &cli.Command{
	Name:  "daemon",
	Usage: "Daemon commands",
	Subcommands: []*cli.Command{
		supervisorDaemonStartCmd,
	},
}

var supervisorDaemonStartCmd = &cli.Command{
	Name:  "start",
	Usage: "Start titan supervisor node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "server-url",
			Usage: "Topology Server URL to poll",
			Value: "http://127.0.0.1:8080/topology",
		},
		&cli.StringFlag{
			Name:  "tags",
			Usage: "Comma-separated list of tags this node should run (e.g. edge,candidate)",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		repoPath, err := getRepoPath(cctx)
		if err != nil {
			return err
		}

		serverUrl := cctx.String("server-url")
		tagsStr := cctx.String("tags")
		var allowedTags []string
		if tagsStr != "" {
			parts := strings.Split(tagsStr, ",")
			for _, p := range parts {
				trimmed := strings.TrimSpace(p)
				if trimmed != "" {
					allowedTags = append(allowedTags, trimmed)
				}
			}
		}

		return supervisor.StartDaemon(cctx.Context, repoPath, serverUrl, allowedTags)
	},
}
