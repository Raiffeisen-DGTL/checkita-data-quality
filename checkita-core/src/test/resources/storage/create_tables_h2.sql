DROP TABLE IF EXISTS companies;
CREATE TABLE companies
(
    rank             INTEGER,
    profile          TEXT,
    name             TEXT,
    url              TEXT,
    state            TEXT,
    revenue          TEXT,
    growth_pct       DOUBLE PRECISION,
    industry         TEXT,
    workers          INTEGER,
    previous_workers INTEGER,
    founded          INTEGER,
    yrs_on_list      INTEGER,
    metro            TEXT,
    city             TEXT
);

DROP TABLE IF EXISTS earthquakes;
CREATE TABLE earthquakes
(
    time            TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    depth           DOUBLE PRECISION,
    mag             DOUBLE PRECISION,
    magType         TEXT,
    nst             INTEGER,
    gap             DOUBLE PRECISION,
    dmin            DOUBLE PRECISION,
    rms             DOUBLE PRECISION,
    net             TEXT,
    id              TEXT,
    updated         TEXT,
    place           TEXT,
    type            TEXT,
    horizontalError DOUBLE PRECISION,
    depthError      DOUBLE PRECISION,
    magError        DOUBLE PRECISION,
    magNst          INTEGER,
    status          TEXT,
    locationSource  TEXT,
    magSource       TEXT
);