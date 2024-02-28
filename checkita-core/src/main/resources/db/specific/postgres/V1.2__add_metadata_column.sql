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
    UNIQUE ("job_id", "metric_id", "reference_date")
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
         null,
         "source_id",
         "column_names",
         "params",
         "result",
         "additional_result",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_metric_regular_backup";
DROP TABLE "${defaultSchema}"."results_metric_regular_backup";


ALTER TABLE "${defaultSchema}"."results_metric_composed" RENAME TO "results_metric_composed_backup";
CREATE TABLE "${defaultSchema}"."results_metric_composed"
(
    "job_id"            TEXT             NOT NULL,
    "metric_id"         TEXT             NOT NULL,
    "metric_name"       TEXT             NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         TEXT             NOT NULL,
    "formula"           TEXT             NOT NULL,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" TEXT,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);
INSERT INTO "${defaultSchema}"."results_metric_composed" (
    "job_id",
    "metric_id",
    "metric_name",
    "description",
    "metadata",
    "source_id",
    "formula",
    "result",
    "additional_result",
    "reference_date",
    "execution_date"
) SELECT "job_id",
         "metric_id",
         "metric_name",
         "description",
         null,
         "source_id",
         "formula",
         "result",
         "additional_result",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_metric_composed_backup";
DROP TABLE "${defaultSchema}"."results_metric_composed_backup";


ALTER TABLE "${defaultSchema}"."results_check" RENAME TO "results_check_backup";
CREATE TABLE "${defaultSchema}"."results_check"
(
    "job_id"             TEXT      NOT NULL,
    "check_id"           TEXT      NOT NULL,
    "check_name"         TEXT      NOT NULL,
    "description"        TEXT,
    "metadata"           TEXT,
    "source_id"          TEXT      NOT NULL,
    "base_metric"        TEXT      NOT NULL,
    "compared_metric"    TEXT,
    "compared_threshold" DOUBLE PRECISION,
    "lower_bound"        DOUBLE PRECISION,
    "upper_bound"        DOUBLE PRECISION,
    "status"             TEXT      NOT NULL,
    "message"            TEXT,
    "reference_date"     TIMESTAMP NOT NULL,
    "execution_date"     TIMESTAMP NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date")
);
INSERT INTO "${defaultSchema}"."results_check" (
    "job_id",
    "check_id",
    "check_name",
    "description",
    "metadata",
    "source_id",
    "base_metric",
    "compared_metric",
    "compared_threshold",
    "lower_bound",
    "upper_bound",
    "status",
    "message",
    "reference_date",
    "execution_date"
) SELECT "job_id",
         "check_id",
         "check_name",
         "description",
         null,
         "source_id",
         "base_metric",
         "compared_metric",
         "compared_threshold",
         "lower_bound",
         "upper_bound",
         "status",
         "message",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_check_backup";
DROP TABLE "${defaultSchema}"."results_check_backup";


ALTER TABLE "${defaultSchema}"."results_check_load" RENAME TO "results_check_load_backup";
CREATE TABLE "${defaultSchema}"."results_check_load"
(
    "job_id"         TEXT      NOT NULL,
    "check_id"       TEXT      NOT NULL,
    "check_name"     TEXT      NOT NULL,
    "description"    TEXT,
    "metadata"       TEXT,
    "source_id"      TEXT      NOT NULL,
    "expected"       TEXT      NOT NULL,
    "status"         TEXT      NOT NULL,
    "message"        TEXT,
    "reference_date" TIMESTAMP NOT NULL,
    "execution_date" TIMESTAMP NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date")
);
INSERT INTO "${defaultSchema}"."results_check_load" (
    "job_id",
    "check_id",
    "check_name",
    "description",
    "metadata",
    "source_id",
    "expected",
    "status",
    "message",
    "reference_date",
    "execution_date"
) SELECT "job_id",
         "check_id",
         "check_name",
         null,
         null,
         "source_id",
         "expected",
         "status",
         "message",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_check_load_backup";
DROP TABLE "${defaultSchema}"."results_check_load_backup";