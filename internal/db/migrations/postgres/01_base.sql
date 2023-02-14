CREATE TABLE tags (
    name TEXT PRIMARY KEY,
    created_at INTEGER
);

CREATE TABLE interval_start (
    uuid TEXT PRIMARY KEY,
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

CREATE TABLE interval_tags (
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
