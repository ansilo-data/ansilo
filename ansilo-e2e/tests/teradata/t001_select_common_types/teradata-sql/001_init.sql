CALL testdb.DROP_IF_EXISTS('testdb', 't001__test_tab');
$$

CREATE TABLE t001__test_tab (
    col_char CHAR(1),
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
    col_null CHAR(1)
)
$$

INSERT INTO testdb.t001__test_tab (
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
    'a',
    'foobar',
    '🥑🚀',
    123.456,
    123,
    5432,
    123456,
    -9876543210,
    33.44,
    '424c4f42'XB,
    NEW JSON('{"hello": "🥑"}', UNICODE),
    NEW JSON('{"foo": "bar"}'),
    '2020-12-23',
    '01:02:03',
    TIMESTAMP '2018-02-01 01:02:03',
    TIMESTAMP '1999-01-15 11:00:00+05:00',
    NULL
)