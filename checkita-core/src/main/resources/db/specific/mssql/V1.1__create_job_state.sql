CREATE TABLE "${defaultSchema}"."job_state"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "config"            VARCHAR(MAX)     NOT NULL,
    "reference_date"    DATETIME         NOT NULL,
    "execution_date"    DATETIME         NOT NULL,
    UNIQUE ("job_id", "reference_date")
);