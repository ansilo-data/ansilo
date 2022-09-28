DROP TABLE IF EXISTS t006__identity;
$$

DROP TABLE IF EXISTS t006__default;
$$

CREATE TABLE t006__identity (
    id INT NOT NULL PRIMARY KEY IDENTITY(1, 1),
    data VARCHAR(255)
)
$$

CREATE TABLE t006__default (
    id INT DEFAULT -1,
    data VARCHAR(255)
)
