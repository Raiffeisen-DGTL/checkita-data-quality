ALTER TABLE "${defaultSchema}"."job_state" RENAME TO "job_state_backup";
CREATE TABLE "${defaultSchema}"."job_state"
(
    "job_id"            VARCHAR(512)     NOT NULL,
    "config"            CLOB             NOT NULL,
    "version_info"      VARCHAR(512)     NOT NULL,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "reference_date")
);
INSERT INTO "${defaultSchema}"."job_state" (
    "job_id",
    "config",
    "version_info",
    "reference_date",
    "execution_date"
) SELECT "job_id",
         "config",
         '{"appVersion":"<unknown>","configAPIVersion":"<unknown>"}',
         "reference_date",
         "execution_date"
FROM "${defaultSchema}"."job_state_backup";
DROP TABLE "${defaultSchema}"."job_state_backup";