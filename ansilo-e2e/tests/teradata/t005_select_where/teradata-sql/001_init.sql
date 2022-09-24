CALL testdb.DROP_IF_EXISTS('testdb', 't005__test_tab');
$$

CREATE TABLE t005__test_tab (
    name VARCHAR(255)
)
$$

INSERT INTO t005__test_tab (name) VALUES ('John');
INSERT INTO t005__test_tab (name) VALUES ('Mary');
INSERT INTO t005__test_tab (name) VALUES ('Jane');
