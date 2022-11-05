DROP TABLE IF EXISTS t010__changes;
$$

CREATE TABLE t010__changes (
    id TEXT PRIMARY KEY,
    delta INTEGER
)
$$

INSERT INTO t010__changes VALUES 
('a', -5), ('b', 2), ('c', 4), ('d', 1);
