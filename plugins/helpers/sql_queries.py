class SqlQueries:
    drop_staging_pollution = "DROP TABLE IF EXISTS staging_pollution;"
    drop_staging_metar = "DROP TABLE IF EXISTS staging_metar;"
    drop_cities = "DROP TABLE IF EXISTS cities;"
    drop_facts = "DROP TABLE IF EXISTS facts;"
    drop_time = "DROP TABLE IF EXISTS time;"

    create_staging_pollution = """CREATE TABLE staging_pollution (
        city varchar(256) NOT NULL,
        metric varchar(256),
        dt TIMESTAMP,
        value INTEGER
    );"""

    create_staging_metar = """CREATE TABLE staging_pollution (
        city varchar(256) NOT NULL,
        metric varchar(256),
        dt TIMESTAMP,
        value INTEGER
    );"""

    create_cities = """CREATE TABLE IF NOT EXISTS cities (
        city varchar(256) NOT NULL UNIQUE PRIMARY KEY,
        state varchar(256) NOT NULL,
        latitude FLOAT,
        longitude FLOAT
    );"""

    create_facts = """CREATE TABLE IF NOT EXISTS facts (
        city varchar(256) NOT NULL,
        dt TIMESTAMP NOT NULL,
        pm10 INTEGER,
        pm25 INTEGER,
        temp INTEGER,
        wind_speed INTEGER
    );"""

    create_time = """CREATE TABLE IF NOT EXISTS time (
        dt timestamp NOT NULL UNIQUE,
        hour int4,
        day int4,
        week int4,
        month int4,
        year int4,
        weekday int4,
        CONSTRAINT time_pkey PRIMARY KEY (dt)
    );"""

    facts_table_insert = ("""
        INSERT INTO facts (city, dt, PM10, PM25, temp, wind_speed)
        SELECT
            p.city,
            p.dt,
            AVG(case when p.metric='PM10' then p.value end) as PM10,
            AVG(case when p.metric='PM2.5' then p.value end) as PM25,
            AVG(case when m.metric='temperature' then m.value end) as temp,
            AVG(case when m.metric='wind_speed' then m.value end) as wind_speed
        FROM
            staging_pollution p
        JOIN
            staging_metar m ON (p.dt = m.dt AND p.city = m.city)
        GROUP BY
            p.city, p.dt
        ;
    """)

    time_table_insert = ("""
        INSERT into time
        SELECT
            dt,
            extract(hour from dt),
            extract(day from dt),
            extract(week from dt), 
            extract(month from dt),
            extract(year from dt),
            extract(dayofweek from dt)
        FROM facts;
    """)