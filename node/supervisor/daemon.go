package supervisor

import (
	"context"
	"time"

	"github.com/Filecoin-Titan/titan/node/repo"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("supervisor")

// StartDaemon initializes the repo and starts the main Supervisor control loop.
func StartDaemon(ctx context.Context, repoPath string, serverUrl string, allowedTags []string, logMaxAge, logRotationTime time.Duration, logRotationSize int64) error {
	r, err := repo.NewFS(repoPath)
	if err != nil {
		return err
	}

	ok, err := r.Exists()
	if err != nil {
		return err
	}
	if !ok {
		if err := r.Init(repo.Supervisor); err != nil {
			return err
		}
	}

	lr, err := r.Lock(repo.Supervisor)
	if err != nil {
		return err
	}
	defer lr.Close()

	log.Infof("Supervisor Daemon started at %s", lr.Path())

	// Initialize the Manager that handles the Data Plane (instances/bins)
	manager := NewManager(lr.Path(), serverUrl, allowedTags, logMaxAge, logRotationTime, logRotationSize)
	if err := manager.InitDirs(); err != nil {
		return err
	}

	// For demonstration, we'll poll the mock server every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Do an immediate poll on startup
	manager.PollServer()

	for {
		select {
		case <-ctx.Done():
			log.Info("Supervisor shutting down...")
			manager.StopAll()
			return nil
		case <-ticker.C:
			manager.PollServer()
		}
	}
}
