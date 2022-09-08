DROP TABLE IF EXISTS t006__test_tab;
$$

CREATE TABLE t006__test_tab (
    id INT,
    name VARCHAR(255)
)
$$

INSERT INTO t006__test_tab (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Mary')
