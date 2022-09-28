DROP TABLE IF EXISTS t010__test_tab
$$
DROP TABLE IF EXISTS t010__test_tab_no_pk
$$

CREATE TABLE t010__test_tab (
    id INT PRIMARY KEY,
    name VARCHAR(255)
)
$$

CREATE TABLE t010__test_tab_no_pk (
    id INT,
    name VARCHAR(255)
)
$$

INSERT INTO t010__test_tab (id, name) VALUES (1, 'John') , (2, 'Jane') , (3, 'Mary');
$$

INSERT INTO t010__test_tab_no_pk (id, name) VALUES (1, 'John') , (2, 'Jane') , (3, 'Mary');