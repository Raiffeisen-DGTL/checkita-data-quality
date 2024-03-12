/* no unique constraint for metric error table */
CREATE TABLE "${defaultSchema}"."results_metric_error"
(
    "job_id"            VARCHAR(512)  NOT NULL,
    "metric_id"         VARCHAR(512)  NOT NULL,
    "source_id"         VARCHAR(512),
    "source_key_fields" VARCHAR(2048),
    "metric_columns"    VARCHAR(2048),
    "status"            VARCHAR(512)  NOT NULL,
    "message"           VARCHAR(MAX)  NOT NULL,
    "row_data"          VARCHAR(MAX)  NOT NULL,
    "reference_date"    DATETIME      NOT NULL,
    "execution_date"    DATETIME      NOT NULL
);