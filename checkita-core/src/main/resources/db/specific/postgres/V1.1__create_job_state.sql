CREATE TABLE "${defaultSchema}"."job_state"
(
    "job_id"            TEXT             NOT NULL,
    "config"            TEXT             NOT NULL,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "reference_date")
);