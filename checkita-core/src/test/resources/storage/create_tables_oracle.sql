CREATE TABLE companies
(
    rank             NUMBER(38),
    profile          CLOB,
    name             CLOB,
    url              CLOB,
    state            CLOB,
    revenue          CLOB,
    growth_pct       DOUBLE PRECISION,
    industry         CLOB,
    workers          NUMBER(38),
    previous_workers NUMBER(38),
    founded          NUMBER(38),
    yrs_on_list      NUMBER(38),
    metro            CLOB,
    city             CLOB
);

CREATE TABLE earthquakes
(
    time            CLOB,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    depth           DOUBLE PRECISION,
    mag             DOUBLE PRECISION,
    magType         CLOB,
    nst             NUMBER(38),
    gap             DOUBLE PRECISION,
    dmin            DOUBLE PRECISION,
    rms             DOUBLE PRECISION,
    net             CLOB,
    id              CLOB,
    updated         CLOB,
    place           CLOB,
    type            CLOB,
    horizontalError DOUBLE PRECISION,
    depthError      DOUBLE PRECISION,
    magError        DOUBLE PRECISION,
    magNst          NUMBER(38),
    status          CLOB,
    locationSource  CLOB,
    magSource       CLOB
);