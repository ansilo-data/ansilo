CALL testdb.DROP_IF_EXISTS('testdb', 't013__no_supported_cols');
$$
CALL testdb.DROP_IF_EXISTS('testdb', 't013__one_supported_cols');
$$

CREATE TABLE t013__no_supported_cols (
    arr INTERVAL MINUTE TO SECOND
)
$$

CREATE TABLE t013__one_supported_cols (
    str VARCHAR(255),
    arr INTERVAL MINUTE TO SECOND
)
