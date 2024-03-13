CREATE TABLE "results_metric_error"
(
    "job_id"            TEXT      NOT NULL,
    "metric_id"         TEXT      NOT NULL,
    "source_id"         TEXT,
    "source_key_fields" TEXT,
    "metric_columns"    TEXT,
    "status"            TEXT      NOT NULL,
    "message"           TEXT      NOT NULL,
    "row_data"          TEXT      NOT NULL,
    "error_hash"        TEXT      NOT NULL,
    "reference_date"    TIMESTAMP NOT NULL,
    "execution_date"    TIMESTAMP NOT NULL,
    UNIQUE ("job_id", "error_hash", "reference_date") ON CONFLICT REPLACE
);