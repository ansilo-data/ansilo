DROP TABLE IF EXISTS t004__test_tab;
$$

CREATE TABLE t004__test_tab (
    col VARCHAR(255)
)
$$

INSERT INTO t004__test_tab (col) VALUES ('FOO')
$$

INSERT INTO t004__test_tab (col) VALUES ('BAR')
