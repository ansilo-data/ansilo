DROP TABLE IF EXISTS t005__test_tab;
$$

CREATE TABLE t005__test_tab (
    name VARCHAR(255)
)
$$

INSERT INTO t005__test_tab (name) VALUES ('John'), ('Mary'), ('Jane')
