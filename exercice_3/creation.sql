-- SCHEMA: nyc_taxi

-- DROP SCHEMA IF EXISTS nyc_taxi ;

CREATE SCHEMA IF NOT EXISTS nyc_taxi
    AUTHORIZATION dw_user;


-- Table: nyc_taxi.t_taxi_jaune

-- DROP TABLE IF EXISTS nyc_taxi.t_taxi_jaune;

CREATE TABLE nyc_taxi.t_taxi_jaune (
                                       tpep_pickup_datetime   TIMESTAMPTZ,
                                       tpep_dropoff_datetime  TIMESTAMPTZ,
                                       passenger_count        INTEGER,
                                       trip_distance          DOUBLE PRECISION,
                                       "RatecodeID"           INTEGER,
                                       store_and_fwd_flag     TEXT,
                                       "PULocationID"         INTEGER,
                                       "DOLocationID"         INTEGER,
                                       payment_type           INTEGER,
                                       fare_amount            DOUBLE PRECISION,
                                       extra                  DOUBLE PRECISION,
                                       mta_tax                DOUBLE PRECISION,
                                       tip_amount             DOUBLE PRECISION,
                                       tolls_amount           DOUBLE PRECISION,
                                       improvement_surcharge  DOUBLE PRECISION,
                                       total_amount           DOUBLE PRECISION,
                                       congestion_surcharge   DOUBLE PRECISION,
                                       airport_fee            DOUBLE PRECISION
);



-- Table: nyc_taxi.t_zone

-- DROP TABLE IF EXISTS config.t_zone;

CREATE TABLE IF NOT EXISTS nyc_taxi.t_zone
(
    locationid integer NOT NULL,
    borough character varying COLLATE pg_catalog."default",
    zone character varying COLLATE pg_catalog."default",
    service_zone character varying COLLATE pg_catalog."default",
    CONSTRAINT t_zone_pkey PRIMARY KEY (locationid)
    );


--- Creation du schema config et de la table dimension_temps
-- SCHEMA: config

-- DROP SCHEMA IF EXISTS config ;

CREATE SCHEMA IF NOT EXISTS config
    AUTHORIZATION dw_user;





-- Table: config.t_dimension_temps

-- DROP TABLE IF EXISTS config.t_dimension_temps;

CREATE TABLE IF NOT EXISTS config.t_dimension_temps
(
    jour date NOT NULL,
    timespan_jour bigint,
    end_date_jour date,
    mois character varying(10) COLLATE pg_catalog."default",
    timespan_mois bigint,
    end_date_mois date,
    annee integer,
    timespan_annee bigint,
    end_date_annee date,
    semaine character varying(10) COLLATE pg_catalog."default",
    timespan_semaine bigint,
    end_date_semaine date,
    ferie integer,
    periode_hebdo character varying(10) COLLATE pg_catalog."default",
    periode_trimestrielle character varying(10) COLLATE pg_catalog."default",
    mois_num integer,
    semaine_num integer,
    periode_mensuelle character varying(10) COLLATE pg_catalog."default",
    lib_periode_mois character varying(10) COLLATE pg_catalog."default",
    lib_periode_semaine character varying(10) COLLATE pg_catalog."default",
    semaine_num_iso character varying(10) COLLATE pg_catalog."default",
    jourouvrable integer,
    CONSTRAINT t_dimension_temps_pkey PRIMARY KEY (jour)
    )

