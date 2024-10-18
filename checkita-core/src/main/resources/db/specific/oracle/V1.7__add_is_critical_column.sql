ALTER TABLE "${defaultSchema}"."results_check" RENAME TO "results_check_backup";
CREATE TABLE "${defaultSchema}"."results_check"
(
    "job_id"             VARCHAR(512) NOT NULL,
    "check_id"           VARCHAR(512) NOT NULL,
    "check_name"         VARCHAR(512) NOT NULL,
    "description"        CLOB,
    "metadata"           CLOB,
    "source_id"          VARCHAR(512) NOT NULL,
    "base_metric"        VARCHAR(512) NOT NULL,
    "compared_metric"    CLOB,
    "compared_threshold" DOUBLE PRECISION,
    "lower_bound"        DOUBLE PRECISION,
    "upper_bound"        DOUBLE PRECISION,
    "status"             VARCHAR(512) NOT NULL,
    "message"            CLOB,
    "is_critical"        VARCHAR(512) NOT NULL,
    "reference_date"     TIMESTAMP    NOT NULL,
    "execution_date"     TIMESTAMP    NOT NULL,
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
         "metadata",
         "source_id",
         "base_metric",
         "compared_metric",
         "compared_threshold",
         "lower_bound",
         "upper_bound",
         "status",
         "message",
         false,
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
    "description"    CLOB,
    "metadata"       CLOB,
    "source_id"      VARCHAR(512) NOT NULL,
    "expected"       VARCHAR(512) NOT NULL,
    "status"         VARCHAR(512) NOT NULL,
    "message"        CLOB,
    "is_critical"    VARCHAR(512) NOT NULL,
    "reference_date" TIMESTAMP    NOT NULL,
    "execution_date" TIMESTAMP    NOT NULL,
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
         "description",
         "metadata",
         "source_id",
         "expected",
         "status",
         "message",
         false,
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."results_check_load_backup";
DROP TABLE "${defaultSchema}"."results_check_load_backup";