ALTER TABLE "results_check" RENAME TO "results_check_backup";
CREATE TABLE "results_check"
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
    "is_critical"        BOOLEAN   NOT NULL,
    "reference_date"     TIMESTAMP NOT NULL,
    "execution_date"     TIMESTAMP NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date") ON CONFLICT REPLACE
);
INSERT INTO "results_check" (
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
FROM "results_check_backup";
DROP TABLE "results_check_backup";


ALTER TABLE "results_check_load" RENAME TO "results_check_load_backup";
CREATE TABLE "results_check_load"
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
    "is_critical"    BOOLEAN   NOT NULL,
    "reference_date" TIMESTAMP NOT NULL,
    "execution_date" TIMESTAMP NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date") ON CONFLICT REPLACE
);
INSERT INTO "results_check_load" (
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
FROM "results_check_load_backup";
DROP TABLE "results_check_load_backup";