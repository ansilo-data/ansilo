DROP TABLE IF EXISTS t007__test_tab;
$$

CREATE TABLE t007__test_tab (
    id INT,
    name VARCHAR(255)
)
$$

INSERT INTO t007__test_tab (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Mary')
