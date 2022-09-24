CALL testdb.DROP_IF_EXISTS('testdb', 't012__test_source');
$$
CALL testdb.DROP_IF_EXISTS('testdb', 't012__test_target');
$$

CREATE TABLE t012__test_source (
    id INT,
    name VARCHAR(255)
)
$$

CREATE TABLE t012__test_target (
    id INT,
    name VARCHAR(255),
    source VARCHAR(255),
    created_at TIMESTAMP 
)
$$

INSERT INTO t012__test_source (id, name) VALUES (1, 'John');
INSERT INTO t012__test_source (id, name) VALUES (2, 'Emma');
INSERT INTO t012__test_source (id, name) VALUES (3, 'Jane');
