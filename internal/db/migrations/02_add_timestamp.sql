ALTER TABLE intervals ADD COLUMN created_at INTEGER;
ALTER TABLE intervals ADD COLUMN updated_at INTEGER;
UPDATE intervals SET created_at = unixepoch('2022-08-01T00:00:00Z');
UPDATE intervals SET updated_at = unixepoch('2022-08-01T00:00:00Z'); 
ALTER TABLE tags ADD COLUMN created_at INTEGER;
UPDATE intervals SET created_at = unixepoch('2022-08-01T00:00:00Z');
CREATE TABLE new_interval_tags (
    interval_id INTEGER,
    tag TEXT,
    created_at INTEGER,
    deleted_at INTEGER,
    PRIMARY KEY(interval_id, tag, deleted_at)
    FOREIGN KEY (interval_id) REFERENCES intervals(id),
    FOREIGN KEY (tag) REFERENCES tags(name)
);
INSERT INTO new_interval_tags (interval_id, tag, created_at, deleted_at)
    SELECT interval_id, tag, unixepoch('2022-08-01T00:00:00Z'), NULL FROM interval_tags;
DROP TABLE interval_tags;
ALTER TABLE new_interval_tags RENAME TO interval_tags;
