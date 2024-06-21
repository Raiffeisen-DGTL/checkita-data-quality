CREATE TABLE "${defaultSchema}"."results_metric_trend"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         VARCHAR(512)     NOT NULL,
    "params"            TEXT,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);