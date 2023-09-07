/*
 SEARCH ADN REPLACE schema_name WITH ACTUAL SCHEMA NAME
 */

DROP TABLE IF EXISTS schema_name.results_metric_columnar;
CREATE TABLE schema_name.results_metric_columnar
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
    UNIQUE (job_id, metric_id, reference_date)
);

DROP TABLE IF EXISTS schema_name.results_metric_file;
CREATE TABLE schema_name.results_metric_file
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
    UNIQUE (job_id, metric_id, reference_date)
);

DROP TABLE IF EXISTS schema_name.results_metric_composed;
CREATE TABLE schema_name.results_metric_composed
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
    UNIQUE (job_id, metric_id, reference_date)
);

DROP TABLE IF EXISTS schema_name.results_check;
CREATE TABLE schema_name.results_check
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
    UNIQUE (job_id, check_id, reference_date)
);

DROP TABLE IF EXISTS schema_name.results_check_load;
CREATE TABLE schema_name.results_check_load
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
    UNIQUE (job_id, check_id, reference_date)
);

CREATE OR REPLACE FUNCTION schema_name.upsert_colmet()
    RETURNS trigger AS
$upsert_colmet$
declare
    existing record;
begin
    if (select EXISTS(SELECT 1
                      FROM schema_name.results_metric_columnar
                      WHERE job_id = NEW.job_id
                        AND metric_id = NEW.metric_id
                        AND reference_date = NEW.reference_date)
    ) then
        UPDATE schema_name.results_metric_columnar
        SET metric_name       = NEW.metric_name,
            description       = NEW.description,
            column_names      = NEW.column_names,
            params            = NEW.params,
            result            = NEW.result,
            source_id         = NEW.source_id,
            additional_result = NEW.additional_result,
            execution_date    = NEW.execution_date
        WHERE job_id = NEW.job_id
          AND metric_id = NEW.metric_id
          AND reference_date = NEW.reference_date;
        return null;
    end if;
    return new;
end
$upsert_colmet$
    LANGUAGE plpgsql;

create trigger column_metrics_insert
    before insert
    on schema_name.results_metric_columnar
    for each row
execute procedure schema_name.upsert_colmet();

CREATE OR REPLACE FUNCTION schema_name.upsert_filemet()
    RETURNS trigger AS
$upsert_filemet$
declare
    existing record;
begin
    if (select EXISTS(SELECT 1
                      FROM schema_name.results_metric_file
                      WHERE job_id = NEW.job_id
                        AND metric_id = NEW.metric_id
                        AND reference_date = NEW.reference_date)
    ) then
        UPDATE schema_name.results_metric_file
        SET metric_name       = NEW.metric_name,
            description       = NEW.description,
            source_id         = NEW.source_id,
            result            = NEW.result,
            additional_result = NEW.additional_result,
            execution_date    = NEW.execution_date
        WHERE job_id = NEW.job_id
          AND metric_id = NEW.metric_id
          AND reference_date = NEW.reference_date;
        return null;
    end if;
    return new;
end
$upsert_filemet$
    LANGUAGE plpgsql;

create trigger file_metrics_insert
    before insert
    on schema_name.results_metric_file
    for each row
execute procedure schema_name.upsert_filemet();

CREATE OR REPLACE FUNCTION schema_name.upsert_compmet()
    RETURNS trigger AS
$upsert_compmet$
declare
    existing record;
begin
    if (select EXISTS(SELECT 1
                      FROM schema_name.results_metric_composed
                      WHERE job_id = NEW.job_id
                        AND metric_id = NEW.metric_id
                        AND reference_date = NEW.reference_date)
    ) then
        UPDATE schema_name.results_metric_composed
        SET metric_name       = NEW.metric_name,
            description       = NEW.description,
            source_id         = NEW.source_id,
            formula           = NEW.formula,
            result            = NEW.result,
            additional_result = NEW.additional_result,
            execution_date    = NEW.execution_date
        WHERE job_id = NEW.job_id
          AND metric_id = NEW.metric_id
          AND reference_date = NEW.reference_date;
        return null;
    end if;
    return new;
end
$upsert_compmet$
    LANGUAGE plpgsql;

create trigger composed_metrics_insert
    before insert
    on schema_name.results_metric_composed
    for each row
execute procedure schema_name.upsert_compmet();

CREATE OR REPLACE FUNCTION schema_name.upsert_check()
    RETURNS trigger AS
$upsert_check$
declare
    existing record;
begin
    if (select EXISTS(SELECT 1
                      FROM schema_name.results_check
                      WHERE job_id = NEW.job_id
                        AND check_id = NEW.check_id
                        AND reference_date = NEW.reference_date)
    ) then
        UPDATE schema_name.results_check
        SET check_name         = NEW.check_name,
            description        = NEW.description,
            source_id          = NEW.source_id,
            base_metric        = NEW.base_metric,
            compared_metric    = NEW.compared_metric,
            compared_threshold = NEW.compared_threshold,
            lower_bound        = NEW.lower_bound,
            upper_bound        = NEW.upper_bound,
            status             = NEW.status,
            message            = NEW.message,
            execution_date     = NEW.execution_date
        WHERE job_id = NEW.job_id
          AND check_id = NEW.check_id
          AND reference_date = NEW.reference_date;
        return null;
    end if;
    return new;
end
$upsert_check$
    LANGUAGE plpgsql;

create trigger checks_insert
    before insert
    on schema_name.results_check
    for each row
execute procedure schema_name.upsert_check();


CREATE OR REPLACE FUNCTION schema_name.upsert_check_load()
    RETURNS trigger AS
$upsert_check_load$
declare
    existing record;
begin
    if (select EXISTS(SELECT 1
                      FROM schema_name.results_check_load
                      WHERE job_id = NEW.job_id
                        AND check_id = NEW.check_id
                        AND reference_date = NEW.reference_date)
    ) then
        UPDATE schema_name.results_check_load
        SET source_id      = NEW.source_id,
            check_name     = NEW.check_name,
            expected       = NEW.expected,
            status         = NEW.status,
            message        = NEW.message,
            execution_date = NEW.execution_date
        WHERE job_id = NEW.job_id
          AND check_id = NEW.check_id
          AND reference_date = NEW.reference_date;
        return null;
    end if;
    return new;
end
$upsert_check_load$
    LANGUAGE plpgsql;

create trigger checks_load_insert
    before insert
    on schema_name.results_check_load
    for each row
execute procedure schema_name.upsert_check_load();