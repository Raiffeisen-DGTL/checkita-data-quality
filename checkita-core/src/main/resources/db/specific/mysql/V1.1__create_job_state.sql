/*
    We have to limit number of bytes used for job_id by limiting maximum number of chars to 256.
    This is required because this field is used to build unique index and MySQL limits total size of the key
    in index by 3072 bytes. We cannot reduce size of timestamp field, therefore, have to limit number of chars
    in text field.
 */

CREATE TABLE "${defaultSchema}"."job_state"
(
    "job_id"            VARCHAR(256)     NOT NULL,
    "config"            TEXT             NOT NULL,
    "reference_date"    TIMESTAMP        NOT NULL,
    "execution_date"    TIMESTAMP        NOT NULL,
    UNIQUE ("job_id", "reference_date")
);