package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("orchestrator")

// Storage defines the interface for persisting topology data
type Storage interface {
	GetTopology() ([]byte, error)
	UpdateTopology(data []byte) error
}

// FileStorage implements Storage using a local JSON file
type FileStorage struct {
	Path string
	mu   sync.Mutex
}

func (f *FileStorage) GetTopology() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.Path)
	if err != nil && os.IsNotExist(err) {
		return []byte(`{"instances": {}}`), nil
	}
	return data, err
}

func (f *FileStorage) UpdateTopology(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.WriteFile(f.Path, data, 0644)
}

// RedisStorage implements Storage using Redis
type RedisStorage struct {
	client *redis.Client
	ctx    context.Context
	key    string // Key for topology JSON
}

func NewRedisStorage(url string, customKey string) (*RedisStorage, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opts)
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	key := "titan:orchestrator"
	if customKey != "" {
		key = customKey
	}

	return &RedisStorage{
		client: rdb,
		ctx:    ctx,
		key:    key,
	}, nil
}

func (r *RedisStorage) GetTopology() ([]byte, error) {
	val, err := r.client.Get(r.ctx, r.key).Result()
	if err == redis.Nil {
		return []byte(`{"instances": {}}`), nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

func (r *RedisStorage) UpdateTopology(data []byte) error {
	return r.client.Set(r.ctx, r.key, data, 0).Err()
}

type Orchestrator struct {
	storage Storage
	cache   struct {
		sync.RWMutex
		data []byte
	}
}

func NewOrchestrator(storage Storage) *Orchestrator {
	o := &Orchestrator{
		storage: storage,
	}
	// Initial cache fill
	o.RefreshCache()
	return o
}

func (o *Orchestrator) RefreshCache() {
	data, err := o.storage.GetTopology()
	if err != nil {
		log.Errorf("Failed to refresh cache: %v", err)
		return
	}
	o.cache.Lock()
	o.cache.data = data
	o.cache.Unlock()
}

// StartCacheRefresher periodically updates the local cache from storage
func (o *Orchestrator) StartCacheRefresher() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		o.RefreshCache()
	}
}

// HandleGetTopology returns the cached JSON for Supervisor nodes
func (o *Orchestrator) HandleGetTopology(w http.ResponseWriter, r *http.Request) {
	o.cache.RLock()
	data := o.cache.data
	o.cache.RUnlock()

	if data == nil {
		http.Error(w, "Service initializing", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleDownload serves binary files from the current directory
func (o *Orchestrator) HandleDownload(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Path[len("/download/"):]
	if filename == "" {
		http.Error(w, "Filename required", http.StatusBadRequest)
		return
	}

	// For security, only serve files from the current directory
	if _, err := os.Stat(filename); err != nil {
		log.Warnf("Download failed: file '%s' not found (Request: %s)", filename, r.URL.Path)
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	log.Infof("Serving binary: %s", filename)
	http.ServeFile(w, r, filename)
}

// HandleAdminAPI handles management requests from the Web UI
func (o *Orchestrator) HandleAdminAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		o.HandleGetTopology(w, r)
	case http.MethodPost:
		o.HandleUpdateTopology(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleUpdateTopology saves the new configuration to disk and refreshes cache
func (o *Orchestrator) HandleUpdateTopology(w http.ResponseWriter, r *http.Request) {
	var newTopo interface{}
	if err := json.NewDecoder(r.Body).Decode(&newTopo); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	data, err := json.MarshalIndent(newTopo, "", "    ")
	if err != nil {
		http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
		return
	}

	if err := o.storage.UpdateTopology(data); err != nil {
		http.Error(w, "Failed to save data: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("Topology updated by admin, refreshing local cache")
	o.RefreshCache()
	w.WriteHeader(http.StatusOK)
}
