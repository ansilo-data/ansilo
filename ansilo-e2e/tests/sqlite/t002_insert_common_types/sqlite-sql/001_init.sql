DROP TABLE IF EXISTS t002__test_tab;
$$

CREATE TABLE t002__test_tab (
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
    col_timestamp TIMESTAMP,
    col_timestamp_tz TIMESTAMP,
    col_null NULL
)
$$