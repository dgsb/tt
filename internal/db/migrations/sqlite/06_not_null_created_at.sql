CREATE TABLE new_interval_tags (
    uuid TEXT PRIMARY KEY,
    interval_start_uuid TEXT NOT NULL,
    tag TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(interval_start_uuid) REFERENCES interval_start(uuid),
    FOREIGN KEY(tag) REFERENCES tags(name)
);

INSERT INTO new_interval_tags (uuid, interval_start_uuid, tag, created_at)
SELECT
    interval_tags.uuid,
    interval_tags.interval_start_uuid,
    interval_tags.tag,
    COALESCE(interval_tags.created_at, interval_start.created_at)
FROM interval_tags
    LEFT JOIN interval_start ON interval_tags.interval_start_uuid = interval_start.uuid;

CREATE TABLE new_interval_tags_tombstone (
    uuid TEXT PRIMARY KEY,
    interval_tag_uuid TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(interval_tag_uuid) REFERENCES new_interval_tags(uuid)
);

INSERT INTO new_interval_tags_tombstone (uuid, interval_tag_uuid, created_at)
SELECT uuid, interval_tag_uuid, created_at FROM interval_tags_tombstone;

DROP TABLE interval_tags_tombstone;
ALTER TABLE new_interval_tags_tombstone RENAME TO interval_tags_tombstone;

DROP TABLE interval_tags;
ALTER TABLE new_interval_tags RENAME TO interval_tags;
