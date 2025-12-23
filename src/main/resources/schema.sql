-- StreamLens SQLite Database Schema

-- Connection profiles (credentials stored encrypted)
CREATE TABLE IF NOT EXISTS connections (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL CHECK (type IN ('KAFKA', 'EVENTHUBS')),
    config_encrypted TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Persisted messages
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER,
    offset_value INTEGER,
    message_key TEXT,
    message_value BLOB,
    headers TEXT,
    event_timestamp INTEGER,
    flagged INTEGER DEFAULT 0,
    starred INTEGER DEFAULT 0,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (connection_id) REFERENCES connections(id) ON DELETE CASCADE
);

-- Indexes for message queries
CREATE INDEX IF NOT EXISTS idx_messages_connection_topic
    ON messages(connection_id, topic, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_starred
    ON messages(starred) WHERE starred = 1;
CREATE INDEX IF NOT EXISTS idx_messages_flagged
    ON messages(flagged) WHERE flagged = 1;
CREATE INDEX IF NOT EXISTS idx_messages_timestamp
    ON messages(event_timestamp);

-- Filter presets
CREATE TABLE IF NOT EXISTS filter_presets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    config TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Application settings
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
