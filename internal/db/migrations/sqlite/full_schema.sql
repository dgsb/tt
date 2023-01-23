CREATE TABLE darwin_migrations
                (
                    id             INTEGER  PRIMARY KEY,
                    version        FLOAT    NOT NULL,
                    description    TEXT     NOT NULL,
                    checksum       TEXT     NOT NULL,
                    applied_at     DATETIME NOT NULL,
                    execution_time FLOAT    NOT NULL,
                    UNIQUE         (version)
                );
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE tags (
    name TEXT PRIMARY KEY
, created_at INTEGER);
CREATE TABLE IF NOT EXISTS "intervals" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT UNIQUE NOT NULL,
    start_timestamp INTEGER NOT NULL,
    stop_timestamp INTEGER,
    deleted_at INTEGER,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS "interval_tags" (
    interval_uuid TEXT,
    tag TEXT,
    created_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY(interval_uuid, tag, deleted_at)
    FOREIGN KEY (interval_uuid) REFERENCES "intervals"(uuid),
    FOREIGN KEY (tag) REFERENCES tags(name)
);
CREATE TABLE sync_history (
    sync_timestamp INTEGER PRIMARY KEY
);
