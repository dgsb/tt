CREATE TABLE applications (
    name TEXT PRIMARY KEY
);

CREATE TABLE configurations (
    application_name TEXT,
    configuration_name TEXT,
    configuration_value TEXT,
    PRIMARY KEY (application_name, configuration_name),
    FOREIGN KEY (application_name) REFERENCES applications(name)
);
