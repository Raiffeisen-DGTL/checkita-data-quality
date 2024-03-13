CREATE TABLE "${defaultSchema}"."results_metric_error"
(
    "job_id"            VARCHAR(512)   NOT NULL,
    "metric_id"         VARCHAR(512)   NOT NULL,
    "source_id"         VARCHAR(512),
    "source_key_fields" VARCHAR(2048),
    "metric_columns"    VARCHAR(2048),
    "status"            VARCHAR(512)   NOT NULL,
    "message"           CLOB           NOT NULL,
    "row_data"          CLOB           NOT NULL,
    "error_hash"        VARCHAR(512)   NOT NULL,
    "reference_date"    TIMESTAMP      NOT NULL,
    "execution_date"    TIMESTAMP      NOT NULL,
    UNIQUE ("job_id", "error_hash", "reference_date")
);