-- Orchestrator Database Schema

CREATE TABLE IF NOT EXISTS supervisors (
    id VARCHAR(128) PRIMARY KEY,
    ip VARCHAR(64) NOT NULL,
    version VARCHAR(64) NOT NULL,
    last_heartbeat DATETIME NOT NULL,
    tags TEXT, -- Comma-separated tags
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
    UNIQUE KEY idx_supervisor_instance (supervisor_id, instance_name)
);

-- Table for storing the main topology configuration (worker instances)
CREATE TABLE IF NOT EXISTS topology (
    name VARCHAR(255) PRIMARY KEY,
    content MEDIUMBLOB NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Table for storing node public keys for RSA signature authentication
CREATE TABLE IF NOT EXISTS nodes (
    node_id VARCHAR(128) PRIMARY KEY,
    public_key TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
