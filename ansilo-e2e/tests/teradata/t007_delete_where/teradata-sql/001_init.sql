CALL testdb.DROP_IF_EXISTS('testdb', 't007__test_tab');
$$

CREATE TABLE t007__test_tab (
    id INT NOT NULL,
    name VARCHAR(255),
    PRIMARY KEY (id)
)
$$

INSERT INTO t007__test_tab (id, name) VALUES (1, 'John');
INSERT INTO t007__test_tab (id, name) VALUES (2, 'Jane');
INSERT INTO t007__test_tab (id, name) VALUES (3, 'Mary');
