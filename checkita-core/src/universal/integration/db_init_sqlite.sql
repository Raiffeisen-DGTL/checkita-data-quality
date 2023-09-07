DROP TABLE IF EXISTS results_metric_columnar;
CREATE TABLE results_metric_columnar
(
    job_id            TEXT                     NOT NULL,
    metric_id         TEXT                     NOT NULL,
    metric_name       TEXT                     NOT NULL,
    description       TEXT,
    source_id         TEXT                     NOT NULL,
    column_names      TEXT                     NOT NULL,
    params            TEXT,
    result            DOUBLE PRECISION         NOT NULL,
    additional_result TEXT,
    reference_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (job_id, metric_id, reference_date) ON CONFLICT REPLACE
);

DROP TABLE IF EXISTS results_metric_file;
CREATE TABLE results_metric_file
(
    job_id            TEXT                     NOT NULL,
    metric_id         TEXT                     NOT NULL,
    metric_name       TEXT                     NOT NULL,
    description       TEXT,
    source_id         TEXT                     NOT NULL,
    result            DOUBLE PRECISION         NOT NULL,
    additional_result TEXT,
    reference_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (job_id, metric_id, reference_date) ON CONFLICT REPLACE
);

DROP TABLE IF EXISTS results_metric_composed;
CREATE TABLE results_metric_composed
(
    job_id            TEXT                     NOT NULL,
    metric_id         TEXT                     NOT NULL,
    metric_name       TEXT                     NOT NULL,
    description       TEXT,
    source_id         TEXT,
    formula           TEXT                     NOT NULL,
    result            DOUBLE PRECISION         NOT NULL,
    additional_result TEXT,
    reference_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_date    TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (job_id, metric_id, reference_date) ON CONFLICT REPLACE
);

DROP TABLE IF EXISTS results_check;
CREATE TABLE results_check
(
    job_id             TEXT                     NOT NULL,
    check_id           TEXT                     NOT NULL,
    check_name         TEXT                     NOT NULL,
    description        TEXT,
    source_id          TEXT                     NOT NULL,
    base_metric        TEXT                     NOT NULL,
    compared_metric    TEXT,
    compared_threshold DOUBLE PRECISION,
    lower_bound        TEXT,
    upper_bound        TEXT,
    status             TEXT                     NOT NULL,
    message            TEXT,
    reference_date     TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_date     TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (job_id, check_id, reference_date) ON CONFLICT REPLACE
);

DROP TABLE IF EXISTS results_check_load;
CREATE TABLE results_check_load
(
    job_id         TEXT                     NOT NULL,
    check_id       TEXT                     NOT NULL,
    check_name     TEXT                     NOT NULL,
    source_id      TEXT                     NOT NULL,
    expected       TEXT                     NOT NULL,
    status         TEXT                     NOT NULL,
    message        TEXT,
    reference_date TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (job_id, check_id, reference_date) ON CONFLICT REPLACE
);