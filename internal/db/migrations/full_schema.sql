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
CREATE TABLE intervals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_timestamp INTEGER NOT NULL,
    stop_timestamp INTEGER,
    deleted_at INTEGER,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE tags (
    name TEXT PRIMARY KEY,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS "interval_tags" (
    interval_id INTEGER,
    tag TEXT,
    created_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY(interval_id, tag, deleted_at)
    FOREIGN KEY (interval_id) REFERENCES intervals(id),
    FOREIGN KEY (tag) REFERENCES tags(name)
);
