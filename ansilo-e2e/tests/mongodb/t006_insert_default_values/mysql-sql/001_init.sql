DROP TABLE IF EXISTS t006__auto_increment;
$$

DROP TABLE IF EXISTS t006__default;
$$

CREATE TABLE t006__auto_increment (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    data VARCHAR(255)
)
$$

CREATE TABLE t006__default (
    id INT DEFAULT -1,
    data VARCHAR(255)
)
