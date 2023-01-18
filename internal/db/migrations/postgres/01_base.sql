CREATE TABLE tags (
    name TEXT PRIMARY KEY,
    created_at INTEGER
);
CREATE TABLE intervals (
    id SERIAL PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    start_timestamp INTEGER NOT NULL,
    stop_timestamp INTEGER,
    deleted_at INTEGER,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE interval_tags (
    interval_uuid TEXT,
    tag TEXT,
    created_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY (interval_uuid, tag, deleted_at),
    FOREIGN KEY (interval_uuid) REFERENCES intervals(uuid),
    FOREIGN KEY (tag) REFERENCES tags(name)
);
