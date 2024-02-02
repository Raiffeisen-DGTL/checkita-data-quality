CREATE TABLE "${defaultSchema}"."job_state"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "config"            CLOB             NOT NULL,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "reference_date")
);