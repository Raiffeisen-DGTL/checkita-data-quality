DROP TABLE IF EXISTS companies;
CREATE TABLE companies
(
    rank             INT,
    profile          TEXT,
    name             TEXT,
    url              TEXT,
    state            TEXT,
    revenue          TEXT,
    growth_pct       REAL,
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
    latitude        REAL,
    longitude       REAL,
    depth           REAL,
    mag             REAL,
    magType         TEXT,
    nst             INT,
    gap             REAL,
    dmin            REAL,
    rms             REAL,
    net             TEXT,
    id              TEXT,
    updated         TEXT,
    place           TEXT,
    type            TEXT,
    horizontalError REAL,
    depthError      REAL,
    magError        REAL,
    magNst          INT,
    status          TEXT,
    locationSource  TEXT,
    magSource       TEXT
);