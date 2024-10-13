ALTER TABLE "${defaultSchema}"."results_metric_regular" RENAME TO "results_metric_regular_backup";
CREATE TABLE "${defaultSchema}"."results_metric_regular"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         VARCHAR(512)     NOT NULL,
    "column_names"      TEXT,
    "params"            TEXT,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
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
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       TEXT,
    "metadata"          TEXT,
    "source_id"         VARCHAR(512)     NOT NULL,
    "formula"           TEXT             NOT NULL,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
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
    "job_id"             VARCHAR(512) NOT NULL,
    "check_id"           VARCHAR(512) NOT NULL,
    "check_name"         VARCHAR(512) NOT NULL,
    "description"        TEXT,
    "metadata"           TEXT,
    "source_id"          VARCHAR(512) NOT NULL,
    "base_metric"        VARCHAR(512) NOT NULL,
    "compared_metric"    VARCHAR(512),
    "compared_threshold" DOUBLE PRECISION,
    "lower_bound"        DOUBLE PRECISION,
    "upper_bound"        DOUBLE PRECISION,
    "status"             VARCHAR(512) NOT NULL,
    "message"            TEXT,
    "is_critical"        BOOLEAN   NOT NULL,
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
    "is_critical",
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
         "is_critical",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_check_backup";
DROP TABLE "${defaultSchema}"."results_check_backup";


ALTER TABLE "${defaultSchema}"."results_check_load" RENAME TO "results_check_load_backup";
CREATE TABLE "${defaultSchema}"."results_check_load"
(
    "job_id"         VARCHAR(512) NOT NULL,
    "check_id"       VARCHAR(512) NOT NULL,
    "check_name"     VARCHAR(512) NOT NULL,
    "description"    VARCHAR(512),
    "metadata"       TEXT,
    "source_id"      VARCHAR(512) NOT NULL,
    "expected"       VARCHAR(512) NOT NULL,
    "status"         VARCHAR(512) NOT NULL,
    "message"        TEXT,
    "is_critical"    BOOLEAN   NOT NULL,
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
    "is_critical",
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
         "is_critical",
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_check_load_backup";
DROP TABLE "${defaultSchema}"."results_check_load_backup";