CALL testdb.DROP_IF_EXISTS('testdb', 't009__serial');
$$

CALL testdb.DROP_IF_EXISTS('testdb', 't009__default');
$$

CREATE TABLE t009__serial (
    id INT NOT NULL GENERATED ALWAYS AS IDENTITY
           (START WITH 1 
            INCREMENT BY 1 
            MINVALUE 1 
            MAXVALUE 2147483647 
            NO CYCLE),
    data VARCHAR(255)
)
$$

CREATE TABLE t009__default (
    id INT DEFAULT -1,
    data VARCHAR(255)
)
