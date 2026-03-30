-- Orchestrator Database Schema

CREATE TABLE IF NOT EXISTS supervisors (
    id VARCHAR(128) PRIMARY KEY,
    ip VARCHAR(64) NOT NULL,
    version VARCHAR(64) NOT NULL,
    last_heartbeat DATETIME NOT NULL,
    tags TEXT, -- Comma-separated local tags as reported by the supervisor
    cpu_cores INT DEFAULT 0,
    total_memory_bytes BIGINT UNSIGNED DEFAULT 0,
    total_disk_bytes BIGINT UNSIGNED DEFAULT 0,
    cpu_percent DOUBLE DEFAULT 0,
    used_memory_bytes BIGINT UNSIGNED DEFAULT 0,
    used_disk_bytes BIGINT UNSIGNED DEFAULT 0,
    net_rx_bytes_per_sec BIGINT UNSIGNED DEFAULT 0,
    net_tx_bytes_per_sec BIGINT UNSIGNED DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_last_heartbeat (last_heartbeat),
    KEY idx_updated_at (updated_at)
);

CREATE TABLE IF NOT EXISTS supervisor_instances (
    id INT AUTO_INCREMENT PRIMARY KEY,
    supervisor_id VARCHAR(128) NOT NULL,
    instance_name VARCHAR(128) NOT NULL,
    hash VARCHAR(128) NOT NULL,
    status VARCHAR(64) NOT NULL, -- running, stopped, crashed, updating
    crash_count INT DEFAULT 0,
    last_error TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (supervisor_id) REFERENCES supervisors(id) ON DELETE CASCADE,
    UNIQUE KEY idx_supervisor_instance (supervisor_id, instance_name),
    KEY idx_supervisor_id (supervisor_id)
);

-- Table for storing the main topology configuration (worker instances)
CREATE TABLE IF NOT EXISTS topology (
    name VARCHAR(255) PRIMARY KEY,
    content MEDIUMBLOB NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_updated_at (updated_at)
);

-- Table for storing node public keys for RSA signature authentication
CREATE TABLE IF NOT EXISTS nodes (
    node_id VARCHAR(128) PRIMARY KEY,
    public_key TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Administrator-assigned tags per node.
-- Injected as node_tags[] in the topology response.
-- The Supervisor merges these with its local --tags to determine which instances to run.
-- Server tags take priority: on restart the Supervisor re-reads from server.
CREATE TABLE IF NOT EXISTS node_tags (
    node_id VARCHAR(128) NOT NULL,
    tag VARCHAR(64) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (node_id, tag),
    KEY idx_node_id (node_id)
);

-- Tracks whether an admin has explicitly configured tags for a given node.
-- Needed to distinguish "not configured yet" (fall back to local --tags) from
-- "configured as empty" (server says: run nothing).
CREATE TABLE IF NOT EXISTS node_config (
    node_id VARCHAR(128) PRIMARY KEY,
    tags_configured TINYINT(1) NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_updated_at (updated_at)
);
