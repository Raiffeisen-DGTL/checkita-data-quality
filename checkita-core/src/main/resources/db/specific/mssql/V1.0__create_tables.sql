CREATE TABLE "${defaultSchema}"."results_metric_regular"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       VARCHAR(MAX),
    "source_id"         VARCHAR(512)     NOT NULL,
    "column_names"      VARCHAR(2048),
    "params"            VARCHAR(2048),
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
    "reference_date"    DATETIME         NOT NULL,
    "execution_date"    DATETIME         NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);

CREATE TABLE "${defaultSchema}"."results_metric_composed"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "metric_id"         VARCHAR(512)     NOT NULL,
    "metric_name"       VARCHAR(512)     NOT NULL,
    "description"       VARCHAR(MAX),
    "source_id"         VARCHAR(512)     NOT NULL,
    "formula"           VARCHAR(2048)    NOT NULL,
    "result"            DOUBLE PRECISION NOT NULL,
    "additional_result" VARCHAR(2048),
    "reference_date"    DATETIME         NOT NULL,
    "execution_date"    DATETIME         NOT NULL,
    UNIQUE ("job_id", "metric_id", "reference_date")
);

CREATE TABLE "${defaultSchema}"."results_check"
(
    "job_id"             VARCHAR(512) NOT NULL,
    "check_id"           VARCHAR(512) NOT NULL,
    "check_name"         VARCHAR(512) NOT NULL,
    "description"        VARCHAR(MAX),
    "source_id"          VARCHAR(512) NOT NULL,
    "base_metric"        VARCHAR(512) NOT NULL,
    "compared_metric"    VARCHAR(512),
    "compared_threshold" DOUBLE PRECISION,
    "lower_bound"        DOUBLE PRECISION,
    "upper_bound"        DOUBLE PRECISION,
    "status"             VARCHAR(512) NOT NULL,
    "message"            VARCHAR(MAX),
    "is_critical"        BIT          NOT NULL,
    "reference_date"     DATETIME     NOT NULL,
    "execution_date"     DATETIME     NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date")
);

CREATE TABLE "${defaultSchema}"."results_check_load"
(
    "job_id"         VARCHAR(512) NOT NULL,
    "check_id"       VARCHAR(512) NOT NULL,
    "check_name"     VARCHAR(512) NOT NULL,
    "source_id"      VARCHAR(512) NOT NULL,
    "expected"       VARCHAR(512) NOT NULL,
    "status"         VARCHAR(512) NOT NULL,
    "message"        VARCHAR(MAX),
    "is_critical"    BIT          NOT NULL,
    "reference_date" DATETIME     NOT NULL,
    "execution_date" DATETIME     NOT NULL,
    UNIQUE ("job_id", "check_id", "reference_date")
);