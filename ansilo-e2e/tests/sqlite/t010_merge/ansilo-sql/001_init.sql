IMPORT FOREIGN SCHEMA "t010__%" 
FROM SERVER sqlite INTO public;

CREATE TABLE t010__target (
    id CHAR(1) PRIMARY KEY,
    counter INTEGER
);

INSERT INTO t010__target VALUES ('a', 3), ('b', 1), ('c', 3);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
