CREATE TABLE "${defaultSchema}"."results_job_config"
(
    "job_id"            TEXT             NOT NULL,
    "config"            TEXT             NOT NULL,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "reference_date")
);