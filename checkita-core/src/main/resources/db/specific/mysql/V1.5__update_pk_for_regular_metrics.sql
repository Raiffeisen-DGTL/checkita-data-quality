/*
    We have to limit number of bytes used for job_id, metric_id and check_id by limiting maximum number of chars to 256.
    This is required because these fields are used to build unique index and MySQL limits total size of the key
    in index by 3072 bytes. We cannot reduce size of timestamp field, therefore, have to limit number of chars
    in text fields.
*/

ALTER TABLE "${defaultSchema}"."results_metric_regular" RENAME TO "results_metric_regular_backup";
CREATE TABLE "${defaultSchema}"."results_metric_regular"
(
    "job_id"            VARCHAR(256)     NOT NULL,
    "metric_id"         VARCHAR(256)     NOT NULL,
    "metric_name"       VARCHAR(128)     NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         VARCHAR(512)     NOT NULL,
    "column_names"      TEXT,
    "params"            TEXT,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "metric_id", "metric_name", "reference_date")
);
INSERT INTO "${defaultSchema}"."results_metric_regular" (
    "job_id",
    "metric_id",
    "metric_name",
    "description",
    "metadata",
    "source_id",
    "column_names",
    "params",
    "result",
    "additional_result",
    "reference_date",
    "execution_date"
) SELECT "job_id",
         "metric_id",
         "metric_name",
         "description",
         "metadata",
         "source_id",
         "column_names",
         "params",
         "result",
         "additional_result",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_metric_regular_backup";

DROP TABLE "${defaultSchema}"."results_metric_regular_backup";