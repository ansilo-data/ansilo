DROP TABLE IF EXISTS t009__test_source;
$$
DROP TABLE IF EXISTS t009__test_target;
$$

CREATE TABLE t009__test_source (
    id INT,
    name VARCHAR(255)
)
$$

CREATE TABLE t009__test_target (
    id INT,
    name VARCHAR(255),
    source VARCHAR(255),
    created_at TIMESTAMP 
)
$$

INSERT INTO t009__test_source (id, name)
VALUES (1, 'John'), (2, 'Emma'), (3, 'Jane');
