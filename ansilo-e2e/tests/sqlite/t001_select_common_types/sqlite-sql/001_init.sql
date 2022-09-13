DROP TABLE IF EXISTS t001__test_tab;
$$

CREATE TABLE t001__test_tab (
    col_char CHAR(1),
    col_varchar VARCHAR(255),
    col_decimal DECIMAL(30, 5),
    col_int8 TINYINT,
    col_int16 SMALLINT,
    col_int32 INT,
    col_int64 BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_blob BLOB,
    col_date DATE,
    col_time TIME,
    col_datetime DATETIME,
    col_timestamp TIMESTAMP,
    col_null NULL
)
$$

INSERT INTO t001__test_tab (
    col_char,
    col_varchar,
    col_decimal,
    col_int8,
    col_int16,
    col_int32,
    col_int64,
    col_float,
    col_double,
    col_blob,
    col_date,
    col_time,
    col_datetime,
    col_timestamp,
    col_null
) VALUES (
    'A',
    'foobar',
    123.456,
    88,
    5432,
    123456,
    -9876543210,
    11.22,
    33.44,
    'BLOB',
    '2020-12-23',
    '01:02:03',
    '2018-02-01 01:02:03',
    '1999-01-15T11:00:00+00:00',
    NULL
)