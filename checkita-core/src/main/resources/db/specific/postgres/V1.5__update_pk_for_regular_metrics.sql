ALTER TABLE "${defaultSchema}"."results_metric_regular" RENAME TO "results_metric_regular_backup";
CREATE TABLE "${defaultSchema}"."results_metric_regular"
(
    "job_id"            TEXT             NOT NULL,
    "metric_id"         TEXT             NOT NULL,
    "metric_name"       TEXT             NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         TEXT             NOT NULL,
    "column_names"      TEXT,
    "params"            TEXT,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" TEXT,
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