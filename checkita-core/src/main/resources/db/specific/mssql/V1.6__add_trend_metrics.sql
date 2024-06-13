CREATE TABLE "${defaultSchema}"."results_metric_trend"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       VARCHAR(MAX),
    "metadata"          VARCHAR(MAX),
    "source_id"         VARCHAR(512)     NOT NULL,
    "params"            VARCHAR(MAX),
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
    "reference_date"    DATETIME         NOT NULL,
    "execution_date"    DATETIME         NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);