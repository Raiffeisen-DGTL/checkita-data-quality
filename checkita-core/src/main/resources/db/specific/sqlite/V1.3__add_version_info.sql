ALTER TABLE "job_state" RENAME TO "job_state_backup";
CREATE TABLE "job_state"
(
    "job_id"            TEXT             NOT NULL,
    "config"            TEXT             NOT NULL,
    "version_info"      TEXT             NOT NULL,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "reference_date") ON CONFLICT REPLACE
);
INSERT INTO "job_state" (
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
FROM "job_state_backup";
DROP TABLE "job_state_backup";