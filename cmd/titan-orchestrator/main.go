package main

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/lib/titanlog"
	"github.com/Filecoin-Titan/titan/node/orchestrator"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

var log = logging.Logger("orchestrator")

//go:embed web/*
var webFiles embed.FS

const (
	FlagPort         = "port"
	FlagTopologyPath = "topology-path"
	FlagRedisURL     = "redis-url"
	FlagConfig       = "config"
)

type Config struct {
	Port         string `yaml:"Port"`
	TopologyPath string `yaml:"TopologyPath"`
	Redis        struct {
		Host string `yaml:"Host"`
		Pass string `yaml:"Pass"`
		Key  string `yaml:"Key"`
	} `yaml:"Redis"`
}

func main() {
	types.RunningNodeType = types.NodeScheduler // Reusing an existing type or default
	titanlog.SetupLogLevels()

	app := &cli.App{
		Name:    "titan-orchestrator",
		Usage:   "Central control server for Titan supervisors",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  FlagPort,
				Value: "8088",
				Usage: "Port to listen on (ignored if config file has Port)",
			},
			&cli.StringFlag{
				Name:  FlagTopologyPath,
				Value: "topology.json",
				Usage: "Path to the topology JSON file (File mode only, ignored if config file has TopologyPath)",
			},
			&cli.StringFlag{
				Name:  FlagRedisURL,
				Value: "",
				Usage: "Redis URL (e.g. redis://127.0.0.1:6379/0) to enable distributed mode",
			},
			&cli.StringFlag{
				Name:  FlagConfig,
				Value: "",
				Usage: "Path to a YAML config file for all settings",
			},
		},
		Action: runServer,
	}

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}
}

func runServer(cctx *cli.Context) error {
	port := cctx.String(FlagPort)
	topologyPath := cctx.String(FlagTopologyPath)
	redisURL := cctx.String(FlagRedisURL)
	configPath := cctx.String(FlagConfig)

	var storage orchestrator.Storage
	var err error

	// 1. Try to load from Config file if provided
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return fmt.Errorf("failed to read config file: %v", err)
		}
		var cfg Config
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return fmt.Errorf("failed to parse config file: %v", err)
		}

		// Config file values take precedence
		if cfg.Port != "" {
			port = cfg.Port
		}
		if cfg.TopologyPath != "" {
			topologyPath = cfg.TopologyPath
		}

		if cfg.Redis.Host != "" {
			log.Infof("Using Redis settings from configuration file")
			// Construct URL from Host and Pass
			proto := "redis"
			auth := ""
			if cfg.Redis.Pass != "" {
				auth = ":" + cfg.Redis.Pass + "@"
			}
			redisURL = fmt.Sprintf("%s://%s%s/0", proto, auth, cfg.Redis.Host)
			storage, err = orchestrator.NewRedisStorage(redisURL, cfg.Redis.Key)
			if err != nil {
				return fmt.Errorf("failed to connect to redis from config: %v", err)
			}
		}
	}

	// 2. Fallback to CLI flag if not loaded from config
	if storage == nil && redisURL != "" {
		log.Infof("Initializing Redis storage from CLI: %s", redisURL)
		storage, err = orchestrator.NewRedisStorage(redisURL, "")
		if err != nil {
			return fmt.Errorf("failed to connect to redis: %v", err)
		}
	}

	// 3. Fallback to File storage
	if storage == nil {
		log.Infof("Initializing File storage: %s", topologyPath)
		storage = &orchestrator.FileStorage{Path: topologyPath}
	}

	server := orchestrator.NewOrchestrator(storage)

	mux := http.NewServeMux()

	// 1. Service API for Supervisors
	mux.HandleFunc("/topology", server.HandleGetTopology)
	mux.HandleFunc("/download/", server.HandleDownload)

	// 2. Management API for Web UI
	mux.HandleFunc("/api/config", server.HandleAdminAPI)

	// 3. Static Web UI
	sub, _ := fs.Sub(webFiles, "web")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// 4. Start Background Tasks
	go server.StartCacheRefresher()

	log.Infof("Titan Orchestrator listening on port %s", port)
	if redisURL != "" {
		log.Infof("Distributed mode ACTIVE via Redis")
	} else {
		log.Infof("Managing topology at: %s", topologyPath)
	}

	return http.ListenAndServe(":"+port, mux)
}
