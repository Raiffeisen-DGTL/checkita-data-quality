EXEC sp_rename '${defaultSchema}.results_metric_regular', 'results_metric_regular_backup';
CREATE TABLE "${defaultSchema}"."results_metric_regular"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       VARCHAR(MAX),
    "metadata"          VARCHAR(MAX),
    "source_id"         VARCHAR(512)     NOT NULL,
    "column_names"      VARCHAR(MAX),
    "params"            VARCHAR(MAX),
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
    "reference_date"    DATETIME         NOT NULL,
    "execution_date"    DATETIME         NOT NULL,
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