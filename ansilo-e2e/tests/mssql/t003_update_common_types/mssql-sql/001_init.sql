DROP TABLE IF EXISTS t003__test_tab;
$$

CREATE TABLE t003__test_tab (
    col_char CHAR(1),
    col_nchar NCHAR(2),
    col_varchar VARCHAR(255),
    col_nvarchar NVARCHAR(255),
    col_decimal DECIMAL(30, 5),
    col_uint8 TINYINT,
    col_int16 SMALLINT,
    col_int32 INT,
    col_int64 BIGINT,
    col_float FLOAT(24),
    col_double FLOAT(53),
    col_binary VARBINARY(255),
    col_date DATE,
    col_time TIME,
    col_datetime DATETIME,
    col_datetimeoffset DATETIMEOFFSET,
    col_uuid UNIQUEIDENTIFIER,
    col_null CHAR
)

$$

INSERT INTO t003__test_tab (
    col_char,
    col_nchar,
    col_varchar,
    col_nvarchar,
    col_decimal,
    col_uint8,
    col_int16,
    col_int32,
    col_int64,
    col_float,
    col_double,
    col_binary,
    col_date,
    col_time,
    col_datetime,
    col_datetimeoffset,
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
    NULL
)