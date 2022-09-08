DROP TABLE IF EXISTS t003__test_tab;
$$

CREATE TABLE t003__test_tab (
    col_char CHAR(1),
    col_varchar VARCHAR(255),
    col_text TEXT,
    col_decimal DECIMAL(30, 5),
    col_bool BOOLEAN,
    col_int16 SMALLINT,
    col_int32 INT,
    col_int64 BIGINT,
    col_float REAL,
    col_double DOUBLE PRECISION,
    col_bytea BYTEA,
    col_json JSON,
    col_jsonb JSONB,
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_timestamp_tz TIMESTAMP WITH TIME ZONE,
    col_uuid UUID,
    col_null CHAR
)
$$

INSERT INTO t003__test_tab (
    col_char,
    col_varchar,
    col_text,
    col_decimal,
    col_bool,
    col_int16,
    col_int32,
    col_int64,
    col_float,
    col_double,
    col_bytea,
    col_json,
    col_jsonb,
    col_date,
    col_time,
    col_timestamp,
    col_timestamp_tz,
    col_uuid,
    col_null
) VALUES (
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
)