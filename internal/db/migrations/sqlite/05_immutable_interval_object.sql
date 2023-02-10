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

INSERT INTO interval_start (uuid, start_timestamp, created_at)
SELECT uuid, start_timestamp, created_at FROM intervals ORDER BY start_timestamp;

INSERT INTO interval_stop (uuid, start_uuid, stop_timestamp, created_at)
SELECT uuid(), uuid, stop_timestamp, updated_at
FROM intervals
WHERE stop_timestamp IS NOT NULL;

INSERT INTO interval_tombstone (uuid, start_uuid, created_at)
SELECT uuid(), uuid, deleted_at
FROM intervals
WHERE deleted_at IS NOT NULL;

DROP TABLE intervals;

CREATE TABLE new_interval_tags (
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
    FOREIGN KEY(interval_tag_uuid) REFERENCES new_interval_tags(uuid)
);

INSERT INTO new_interval_tags (uuid, interval_start_uuid, tag, created_at)
SELECT uuid(), interval_uuid, tag, created_at
FROM interval_tags;

INSERT INTO interval_tags_tombstone (uuid, interval_tag_uuid, created_at)
SELECT uuid(), new_interval_tags.uuid, interval_tags.deleted_at
FROM interval_tags
    JOIN new_interval_tags ON interval_tags.interval_uuid = new_interval_tags.interval_start_uuid
WHERE interval_tags.deleted_at IS NOT NULL;

DROP TABLE interval_tags;
ALTER TABLE new_interval_tags RENAME TO interval_tags;
