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
CREATE TABLE tags (
    name TEXT PRIMARY KEY
, created_at INTEGER);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE sync_history (
    sync_timestamp INTEGER PRIMARY KEY
);
CREATE TABLE interval_start (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT UNIQUE NOT NULL,
    start_timestamp INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE interval_stop (
    uuid TEXT PRIMARY KEY,
    start_uuid TEXT UNIQUE NOT NULL,
    stop_timestamp INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (start_uuid) REFERENCES interval_start(uuid)
);
CREATE TABLE interval_tombstone (
    uuid TEXT PRIMARY KEY,
    start_uuid TEXT UNIQUE NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (start_uuid) REFERENCES interval_start(uuid)
);
CREATE TABLE IF NOT EXISTS "interval_tags" (
    uuid TEXT PRIMARY KEY,
    interval_start_uuid TEXT,
    tag TEXT,
    created_at INTEGER,
    FOREIGN KEY(interval_start_uuid) REFERENCES interval_start(uuid),
    FOREIGN KEY(tag) REFERENCES tags(name)
);
CREATE TABLE interval_tags_tombstone (
    uuid TEXT PRIMARY KEY,
    interval_tag_uuid TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(interval_tag_uuid) REFERENCES "interval_tags"(uuid)
);
