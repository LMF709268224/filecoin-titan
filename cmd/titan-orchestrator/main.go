package main

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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
	FlagPort        = "port"
	FlagRedisURL    = "redis-url"
	FlagDatabaseURL = "db-url"
	FlagConfig      = "config"
	FlagAdminUser   = "admin-user"
	FlagAdminPass   = "admin-pass"
	FlagJWTSecret   = "jwt-secret"
)

type Config struct {
	Port  string `yaml:"Port"`
	Redis struct {
		Host string `yaml:"Host"`
		Pass string `yaml:"Pass"`
		Key  string `yaml:"Key"`
	} `yaml:"Redis"`
	Database struct {
		User string `yaml:"User"`
		Pass string `yaml:"Pass"`
		Host string `yaml:"Host"`
		DB   string `yaml:"DB"`
		Key  string `yaml:"Key"`
	} `yaml:"Database"`
	AdminUser string `yaml:"AdminUser"`
	AdminPass string `yaml:"AdminPass"`
	JWTSecret string `yaml:"JWTSecret"`
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
				Usage: "Port to listen on",
			},
			&cli.StringFlag{
				Name:    FlagRedisURL,
				Value:   "",
				Usage:   "Redis URL (e.g. redis://127.0.0.1:6379/0) [REQUIRED]",
				EnvVars: []string{"TITAN_REDIS_URL"},
			},
			&cli.StringFlag{
				Name:    FlagDatabaseURL,
				Value:   "",
				Usage:   "MySQL database URL (e.g. user:pass@tcp(127.0.0.1:3306)/titan) [REQUIRED]",
				EnvVars: []string{"TITAN_DB_URL"},
			},
			&cli.StringFlag{
				Name:  FlagConfig,
				Value: "",
				Usage: "Path to a YAML config file for all settings",
			},
			&cli.StringFlag{
				Name:  FlagAdminUser,
				Value: "",
				Usage: "Admin username for Web UI",
			},
			&cli.StringFlag{
				Name:  FlagAdminPass,
				Value: "",
				Usage: "Admin password for Web UI",
			},
			&cli.StringFlag{
				Name:    FlagJWTSecret,
				Value:   "",
				Usage:   "Secret key for JWT (Admin Web UI) [REQUIRED]",
				EnvVars: []string{"TITAN_JWT_SECRET"},
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
	redisURL := cctx.String(FlagRedisURL)
	dbURL := cctx.String(FlagDatabaseURL)
	configPath := cctx.String(FlagConfig)
	adminUser := cctx.String(FlagAdminUser)
	adminPass := cctx.String(FlagAdminPass)
	jwtSecret := cctx.String(FlagJWTSecret)

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

		if cfg.Port != "" {
			port = cfg.Port
		}

		if cfg.Redis.Host != "" {
			proto := "redis"
			auth := ""
			if cfg.Redis.Pass != "" {
				auth = ":" + cfg.Redis.Pass + "@"
			}
			redisURL = fmt.Sprintf("%s://%s%s/0", proto, auth, cfg.Redis.Host)
		}

		if cfg.Database.Host != "" {
			dbURL = fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
				cfg.Database.User, cfg.Database.Pass, cfg.Database.Host, cfg.Database.DB)
		}

		if cfg.AdminUser != "" {
			adminUser = cfg.AdminUser
		}
		if cfg.AdminPass != "" {
			adminPass = cfg.AdminPass
		}
		if cfg.JWTSecret != "" {
			jwtSecret = cfg.JWTSecret
		}
	}

	// 2. Strict validation
	if redisURL == "" || dbURL == "" || jwtSecret == "" {
		return fmt.Errorf("FATAL: --redis-url, --db-url, and --jwt-secret are all REQUIRED for production stack")
	}

	// 3. Initialize Production Storage
	storage, err := orchestrator.NewProductionStorage(dbURL, redisURL, "")
	if err != nil {
		return fmt.Errorf("failed to initialize production storage: %v", err)
	}

	server := orchestrator.NewOrchestrator(storage)
	server.AdminUser = adminUser
	server.AdminPass = adminPass
	server.JWTSecret = []byte(jwtSecret)

	if server.AdminUser == "admin" && server.AdminPass == "admin123" {
		log.Warn("================================================================")
		log.Warn("SECURITY WARNING: Using default admin/admin123 credentials!")
		log.Warn("Please change ADMIN_USER and ADMIN_PASS in your configuration.")
		log.Warn("================================================================")
	}

	mux := http.NewServeMux()

	// 1. Service API for Supervisors
	mux.HandleFunc("/topology", server.HandleGetTopology)
	mux.HandleFunc("/report", server.HandleReportStatus)

	// Node Authentication & Real-time Sync
	mux.HandleFunc("/api/v1/node/register", server.HandleNodeRegister)
	mux.HandleFunc("/api/v1/node/login", server.HandleNodeLogin)
	mux.HandleFunc("/api/v1/node/ws", server.HandleWS)

	// 2. Management API for Web UI (Protected by JWT)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/api/", server.HandleAdminAPI)

	mux.Handle("/api/", server.JwtAuthMiddleware(adminMux))

	// 3. Static Web UI (Public)
	sub, _ := fs.Sub(webFiles, "web")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// 5. Signal Handling for Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Infof("Titan Orchestrator listening on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen error: %v", err)
		}
	}()

	<-stop
	log.Infof("Shutting down Orchestrator...")
	return srv.Close()
}
