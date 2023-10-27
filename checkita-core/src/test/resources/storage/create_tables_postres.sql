DROP TABLE IF EXISTS companies;
CREATE TABLE companies
(
    rank             INT,
    profile          TEXT,
    name             TEXT,
    url              TEXT,
    state            TEXT,
    revenue          TEXT,
    growth_pct       DOUBLE PRECISION,
    industry         TEXT,
    workers          INT,
    previous_workers INT,
    founded          INT,
    yrs_on_list      INT,
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
    nst             INT,
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
    magNst          INT,
    status          TEXT,
    locationSource  TEXT,
    magSource       TEXT
);


