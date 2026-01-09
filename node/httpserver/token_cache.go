package httpserver

import (
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

// tokenCacheEntry represents a cached token with expiration
type tokenCacheEntry struct {
	payload    *types.TokenPayload
	jwtPayload *types.JWTPayload
	expireAt   time.Time
}

// tokenCache provides a thread-safe cache for verified tokens
type tokenCache struct {
	mu      sync.RWMutex
	entries map[string]*tokenCacheEntry
	maxSize int
}

// newTokenCache creates a new token cache with the specified max size
func newTokenCache(maxSize int) *tokenCache {
	tc := &tokenCache{
		entries: make(map[string]*tokenCacheEntry),
		maxSize: maxSize,
	}

	// Start cleanup goroutine
	go tc.cleanupLoop()

	return tc
}

// Get retrieves a token from the cache
func (tc *tokenCache) Get(token string) (*types.TokenPayload, *types.JWTPayload, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	entry, exists := tc.entries[token]
	if !exists {
		return nil, nil, false
	}

	// Check if expired
	if time.Now().After(entry.expireAt) {
		return nil, nil, false
	}

	return entry.payload, entry.jwtPayload, true
}

// Set stores a token in the cache with TTL
func (tc *tokenCache) Set(token string, payload *types.TokenPayload, jwtPayload *types.JWTPayload, ttl time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Evict old entries if cache is full
	if len(tc.entries) >= tc.maxSize {
		tc.evictOldest()
	}

	tc.entries[token] = &tokenCacheEntry{
		payload:    payload,
		jwtPayload: jwtPayload,
		expireAt:   time.Now().Add(ttl),
	}
}

// Delete removes a token from the cache
func (tc *tokenCache) Delete(token string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.entries, token)
}

// evictOldest removes the oldest entry from the cache
func (tc *tokenCache) evictOldest() {
	var oldestToken string
	var oldestTime time.Time

	for token, entry := range tc.entries {
		if oldestTime.IsZero() || entry.expireAt.Before(oldestTime) {
			oldestTime = entry.expireAt
			oldestToken = token
		}
	}

	if oldestToken != "" {
		delete(tc.entries, oldestToken)
	}
}

// cleanupLoop periodically removes expired entries
func (tc *tokenCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		tc.cleanup()
	}
}

// cleanup removes all expired entries
func (tc *tokenCache) cleanup() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	now := time.Now()
	for token, entry := range tc.entries {
		if now.After(entry.expireAt) {
			delete(tc.entries, token)
		}
	}
}

// Size returns the current number of cached tokens
func (tc *tokenCache) Size() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return len(tc.entries)
}
