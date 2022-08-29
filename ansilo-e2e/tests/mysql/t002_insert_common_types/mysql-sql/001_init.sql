DROP TABLE IF EXISTS t002__test_tab;
$$

CREATE TABLE t002__test_tab (
    col_char CHAR(1),
    col_nchar CHAR(2) CHARACTER SET UTF8MB4,
    col_varchar VARCHAR(255),
    col_nvarchar VARCHAR(255) CHARACTER SET UTF8MB4,
    col_decimal DECIMAL(30, 5),
    col_int8 TINYINT,
    col_int16 SMALLINT,
    col_int32 INT,
    col_int64 BIGINT,
    col_uint8 TINYINT UNSIGNED,
    col_uint16 SMALLINT UNSIGNED,
    col_uint32 INT UNSIGNED,
    col_uint64 BIGINT UNSIGNED,
    col_float FLOAT,
    col_double DOUBLE,
    col_blob BLOB,
    col_json JSON,
    col_date DATE,
    col_time TIME,
    col_datetime DATETIME,
    col_timestamp TIMESTAMP,
    col_null CHAR
)