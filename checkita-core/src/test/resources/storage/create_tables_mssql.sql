DROP TABLE IF EXISTS companies;
CREATE TABLE companies
(
    rank             INT,
    profile          VARCHAR(MAX),
    name             VARCHAR(MAX),
    url              VARCHAR(MAX),
    state            VARCHAR(MAX),
    revenue          VARCHAR(MAX),
    growth_pct       DOUBLE PRECISION,
    industry         VARCHAR(MAX),
    workers          INT,
    previous_workers INT,
    founded          INT,
    yrs_on_list      INT,
    metro            VARCHAR(MAX),
    city             VARCHAR(MAX)
);

DROP TABLE IF EXISTS earthquakes;
CREATE TABLE earthquakes
(
    time            VARCHAR(MAX),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    depth           DOUBLE PRECISION,
    mag             DOUBLE PRECISION,
    magType         VARCHAR(MAX),
    nst             INT,
    gap             DOUBLE PRECISION,
    dmin            DOUBLE PRECISION,
    rms             DOUBLE PRECISION,
    net             VARCHAR(MAX),
    id              VARCHAR(MAX),
    updated         VARCHAR(MAX),
    place           VARCHAR(MAX),
    type            VARCHAR(MAX),
    horizontalError DOUBLE PRECISION,
    depthError      DOUBLE PRECISION,
    magError        DOUBLE PRECISION,
    magNst          INT,
    status          VARCHAR(MAX),
    locationSource  VARCHAR(MAX),
    magSource       VARCHAR(MAX)
);