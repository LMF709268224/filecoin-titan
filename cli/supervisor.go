package cli

import (
	"strconv"
	"strings"
	"time"

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
		&cli.StringFlag{
			Name:  "log-max-age",
			Usage: "Max age of instance logs (e.g. 15d, 30d)",
			Value: "15d",
		},
		&cli.StringFlag{
			Name:  "log-rotation-time",
			Usage: "Interval between log rotations (e.g. 24h, 1h)",
			Value: "24h",
		},
		&cli.StringFlag{
			Name:  "log-rotation-size",
			Usage: "Max size of a single log file (e.g. 100MiB, 1GiB)",
			Value: "100MiB",
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

		logMaxAge, err := parseDuration(cctx.String("log-max-age"))
		if err != nil {
			return err
		}

		logRotationTime, err := time.ParseDuration(cctx.String("log-rotation-time"))
		if err != nil {
			return err
		}

		logRotationSize, err := parseSize(cctx.String("log-rotation-size"))
		if err != nil {
			return err
		}

		return supervisor.StartDaemon(cctx.Context, repoPath, serverUrl, allowedTags, logMaxAge, logRotationTime, logRotationSize)
	},
}

func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		daysStr := strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(daysStr)
		if err != nil {
			return 0, err
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(s)
	unit := int64(1)
	valStr := s

	if strings.HasSuffix(s, "MIB") {
		unit = 1024 * 1024
		valStr = strings.TrimSuffix(s, "MIB")
	} else if strings.HasSuffix(s, "GIB") {
		unit = 1024 * 1024 * 1024
		valStr = strings.TrimSuffix(s, "GIB")
	} else if strings.HasSuffix(s, "KIB") {
		unit = 1024
		valStr = strings.TrimSuffix(s, "KIB")
	} else if strings.HasSuffix(s, "MB") {
		unit = 1000 * 1000
		valStr = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "GB") {
		unit = 1000 * 1000 * 1000
		valStr = strings.TrimSuffix(s, "GB")
	}

	val, err := strconv.ParseInt(strings.TrimSpace(valStr), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * unit, nil
}
