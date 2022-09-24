CALL testdb.DROP_IF_EXISTS('testdb', 't008__people');
$$

CALL testdb.DROP_IF_EXISTS('testdb', 't008__pets');
$$

CREATE TABLE t008__people (
    id INT,
    name VARCHAR(255)
)
$$

CREATE TABLE t008__pets (
    id INT,
    name VARCHAR(255),
    owner_id INT
)
$$

INSERT INTO t008__people (id, name) VALUES (1, 'John');
INSERT INTO t008__people (id, name) VALUES (2, 'Mary');
INSERT INTO t008__people (id, name) VALUES (3, 'Jane');
$$

INSERT INTO t008__pets (id, name, owner_id) VALUES (1, 'Luna', 1);
INSERT INTO t008__pets (id, name, owner_id) VALUES (2, 'Salt', 1);
INSERT INTO t008__pets (id, name, owner_id) VALUES (3, 'Pepper', 3);
INSERT INTO t008__pets (id, name, owner_id) VALUES (4, 'Morris', NULL);
