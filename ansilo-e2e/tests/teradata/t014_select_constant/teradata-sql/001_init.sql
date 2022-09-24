CALL testdb.DROP_IF_EXISTS('testdb', 't014__test_tab');
$$

CREATE TABLE t014__test_tab (
    col VARCHAR(255)
)
$$

INSERT INTO t014__test_tab (col) VALUES ('data');

