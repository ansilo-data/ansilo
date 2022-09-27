DROP TABLE IF EXISTS t004__test_tab;
$$

CREATE TABLE t004__test_tab (
    COL VARCHAR(255)
)
$$

INSERT INTO t004__test_tab (COL) VALUES ('FOO')
$$

INSERT INTO t004__test_tab (COL) VALUES ('BAR')
