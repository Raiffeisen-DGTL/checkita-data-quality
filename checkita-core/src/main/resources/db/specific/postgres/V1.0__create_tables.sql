CREATE TABLE "${defaultSchema}"."results_metric_regular"
(
    "job_id"            TEXT             NOT NULL,
    "metric_id"         TEXT             NOT NULL,
    "metric_name"       TEXT             NOT NULL,
    "description"       TEXT,
    "source_id"         TEXT             NOT NULL,
    "column_names"      TEXT,
    "params"            TEXT,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" TEXT,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);

CREATE TABLE "${defaultSchema}"."results_metric_composed"
(
    "job_id"            TEXT             NOT NULL,
    "metric_id"         TEXT             NOT NULL,
    "metric_name"       TEXT             NOT NULL,
    "description"       TEXT,
    "source_id"         TEXT             NOT NULL,
    "formula"           TEXT             NOT NULL,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" TEXT,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);

CREATE TABLE "${defaultSchema}"."results_check"
(
    "job_id"             TEXT      NOT NULL,
    "check_id"           TEXT      NOT NULL,
    "check_name"         TEXT      NOT NULL,
    "description"        TEXT,
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

CREATE TABLE "${defaultSchema}"."results_check_load"
(
    "job_id"         TEXT      NOT NULL,
    "check_id"       TEXT      NOT NULL,
    "check_name"     TEXT      NOT NULL,
    "source_id"      TEXT      NOT NULL,
    "expected"       TEXT      NOT NULL,
    "status"         TEXT      NOT NULL,
    "message"        TEXT,
    "reference_date" TIMESTAMP NOT NULL,
    "execution_date" TIMESTAMP NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date")
);