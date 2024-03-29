BEGIN
EXECUTE IMMEDIATE 'DROP TABLE T004__TEST_TAB';
EXCEPTION
WHEN OTHERS THEN NULL;
END;
$$

CREATE TABLE T004__TEST_TAB (
    COL_CHAR CHAR(1 CHAR),
    COL_NCHAR NCHAR(2),
    COL_VARCHAR2 VARCHAR2(255),
    COL_NVARCHAR2 NVARCHAR2(255),
    COL_NUMBER NUMBER,
    COL_FLOAT FLOAT,
    COL_INT8 NUMBER(2),
    COL_INT16 NUMBER(4),
    COL_INT32 NUMBER(9),
    COL_INT64 NUMBER(18),
    COL_BINARY_FLOAT BINARY_FLOAT,
    COL_BINARY_DOUBLE BINARY_DOUBLE,
    COL_RAW RAW(255),
    COL_LONG_RAW LONG RAW,
    COL_BLOB BLOB,
    COL_CLOB CLOB,
    COL_NCLOB NCLOB,
    COL_JSON JSON,
    COL_DATE DATE,
    COL_TIMESTAMP TIMESTAMP,
    COL_TIMESTAMP_TZ TIMESTAMP WITH TIME ZONE,
    COL_TIMESTAMP_LTZ TIMESTAMP WITH LOCAL TIME ZONE,
    COL_NULL CHAR
)
$$

INSERT INTO T004__TEST_TAB (
    COL_CHAR,
    COL_NCHAR,
    COL_VARCHAR2,
    COL_NVARCHAR2,
    COL_NUMBER,
    COL_FLOAT,
    COL_INT8,
    COL_INT16,
    COL_INT32,
    COL_INT64,
    COL_BINARY_FLOAT,
    COL_BINARY_DOUBLE,
    COL_RAW,
    COL_LONG_RAW,
    COL_BLOB,
    COL_CLOB,
    COL_NCLOB,
    COL_JSON,
    COL_DATE,
    COL_TIMESTAMP,
    COL_TIMESTAMP_TZ,
    COL_TIMESTAMP_LTZ,
    COL_NULL
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
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
)