CREATE TABLE intervals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_timestamp INTEGER NOT NULL,
    stop_timestamp INTEGER
);

CREATE TABLE tags (
    name TEXT PRIMARY KEY
);

CREATE TABLE interval_tags (
    interval_id INTEGER,
    tag TEXT,
    PRIMARY KEY (interval_id, tag),
    FOREIGN KEY (interval_id) REFERENCES intervals(id),
    FOREIGN KEY (tag) REFERENCES tags(name)
);
