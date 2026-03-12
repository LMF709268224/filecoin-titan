package supervisor

import (
	"context"
	"errors"
	"time"

	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/types"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("supervisor")

// StartDaemon initializes the repo and starts the main Supervisor control loop.
func StartDaemon(ctx context.Context, repoPath string, serverUrl string, allowedTags []string, logMaxAge, logRotationTime time.Duration, logRotationSize int64, platform string) error {
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

	// Handle Node ID persistence
	nodeIDBytes, err := lr.NodeID()
	if err != nil && err != repo.ErrNodeIDNotExist {
		return err
	}

	var nodeID string
	if err == repo.ErrNodeIDNotExist {
		nodeID = "s_" + uuid.NewString()
		log.Infof("Generating new Node ID: %s", nodeID)
		if err := lr.SetNodeID([]byte(nodeID)); err != nil {
			return err
		}
	} else {
		nodeID = string(nodeIDBytes)
		log.Infof("Loaded existing Node ID: %s", nodeID)
	}

	// Handle Identity RSA Key
	keystore, err := lr.KeyStore()
	if err != nil {
		return err
	}

	keyName := "private-key"
	_, err = keystore.Get(keyName)
	if errors.Is(err, types.ErrKeyInfoNotFound) {
		log.Info("Generating new RSA identity key...")
		priv, err := titanrsa.GeneratePrivateKey(1024)
		if err != nil {
			return err
		}

		kInfo := types.KeyInfo{
			Type:       types.KeyType(keyName),
			PrivateKey: titanrsa.PrivateKey2Pem(priv),
		}

		if err := keystore.Put(keyName, kInfo); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	log.Infof("Supervisor Daemon started at %s", lr.Path())

	// Initialize the Manager that handles the Data Plane (instances/bins)
	manager := NewManager(lr.Path(), nodeID, serverUrl, allowedTags, logMaxAge, logRotationTime, logRotationSize, platform, keystore)
	if err := manager.InitDirs(); err != nil {
		return err
	}

	// For demonstration, we'll poll the mock server every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Do an immediate poll on startup
	manager.PullTopology()

	// Start WebSocket watcher for real-time reloads
	go manager.WatchConfig(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("Supervisor shutting down...")
			manager.StopAll()
			return nil
		case <-ticker.C:
			manager.PullTopology()
		}
	}
}
