CREATE TABLE "results_metric_trend"
(
    "job_id"            TEXT             NOT NULL,
    "metric_id"         TEXT             NOT NULL,
    "metric_name"       TEXT             NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         TEXT             NOT NULL,
    "params"            TEXT,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" TEXT,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date") ON CONFLICT REPLACE
);