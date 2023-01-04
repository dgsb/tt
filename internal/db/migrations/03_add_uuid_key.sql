CREATE TABLE new_intervals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT UNIQUE NOT NULL,
    start_timestamp INTEGER NOT NULL,
    stop_timestamp INTEGER,
    deleted_at INTEGER,
    created_at INTEGER,
    updated_at INTEGER
);

INSERT INTO new_intervals (id, uuid, start_timestamp, stop_timestamp, deleted_at, created_at, updated_at)
SELECT id, uuid(), start_timestamp, stop_timestamp, deleted_at, created_at, updated_at FROM intervals;

CREATE TABLE new_interval_tags (
    interval_uuid TEXT,
    tag TEXT,
    created_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY(interval_uuid, tag, deleted_at)
    FOREIGN KEY (interval_uuid) REFERENCES new_intervals(uuid),
    FOREIGN KEY (tag) REFERENCES tags(name)
);

INSERT INTO new_interval_tags (interval_uuid, tag, created_at, deleted_at)
SELECT new_intervals.uuid, interval_tags.tag, interval_tags.created_at, interval_tags.deleted_at
FROM interval_tags
INNER JOIN new_intervals
ON interval_tags.interval_id = new_intervals.id;

DROP TABLE interval_tags;
DROP TABLE intervals;
ALTER TABLE new_intervals RENAME TO intervals;
ALTER TABLE new_interval_tags RENAME TO interval_tags;
