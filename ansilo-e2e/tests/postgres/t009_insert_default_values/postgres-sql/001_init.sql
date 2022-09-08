DROP TABLE IF EXISTS t009__serial;
$$

DROP TABLE IF EXISTS t009__default;
$$

CREATE TABLE t009__serial (
    id SERIAL NOT NULL PRIMARY KEY,
    data VARCHAR(255)
)
$$

CREATE TABLE t009__default (
    id INT DEFAULT -1,
    data VARCHAR(255)
)
