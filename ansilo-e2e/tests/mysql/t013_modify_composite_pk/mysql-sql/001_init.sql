DROP TABLE IF EXISTS t013__test_tab
$$
DROP TABLE IF EXISTS t013__test_tab_no_pk
$$

CREATE TABLE t013__test_tab (
    id1 INT,
    id2 INT,
    name VARCHAR(255),
    PRIMARY KEY (id1, id2)
)
$$

INSERT INTO t013__test_tab (id1, id2, name) VALUES 
(1, 1, 'John') , (1, 2, 'Jane') , (1, 3, 'Mary'),
(2, 1, 'Jack') , (2, 2, 'Jen') , (2, 3, 'Gerald');
