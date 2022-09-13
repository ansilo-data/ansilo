DROP TABLE IF EXISTS t005__people;
$$

DROP TABLE IF EXISTS t005__pets;
$$

CREATE TABLE t005__people (
    id INT,
    name VARCHAR(255)
)
$$

CREATE TABLE t005__pets (
    id INT,
    name VARCHAR(255),
    owner_id INT
)
$$

INSERT INTO t005__people (id, name) 
VALUES (1, 'John'), (2, 'Mary'), (3, 'Jane');
$$

INSERT INTO t005__pets (id, name, owner_id) 
VALUES (1, 'Luna', 1), (2, 'Salt', 1), (3, 'Pepper', 3), (4, 'Morris', NULL);
