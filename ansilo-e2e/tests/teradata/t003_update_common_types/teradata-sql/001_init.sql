CALL testdb.DROP_IF_EXISTS('testdb', 't003__test_tab');
$$

CREATE TABLE t003__test_tab (
    col_char CHAR(1) CHARACTER SET UNICODE,
    col_varchar VARCHAR(255),
    col_clob CLOB CHARACTER SET UNICODE,
    col_decimal DECIMAL(30, 5),
    col_int8 BYTEINT,
    col_int16 SMALLINT,
    col_int32 INT,
    col_int64 BIGINT,
    col_double DOUBLE PRECISION,
    col_blob VARBYTE(100),
    col_json JSON CHARACTER SET UNICODE,
    col_jsonb JSON STORAGE FORMAT BSON,
    col_date DATE,
    col_time TIME,
    col_timestamp TIMESTAMP,
    col_timestamp_tz TIMESTAMP WITH TIME ZONE,
    col_null CHAR
)
$$

INSERT INTO t003__test_tab (
    col_char,
    col_varchar,
    col_clob,
    col_decimal,
    col_int8,
    col_int16,
    col_int32,
    col_int64,
    col_double,
    col_blob,
    col_json,
    col_jsonb,
    col_date,
    col_time,
    col_timestamp,
    col_timestamp_tz,
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
    NULL
)