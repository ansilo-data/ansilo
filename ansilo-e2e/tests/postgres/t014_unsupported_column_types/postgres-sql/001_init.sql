DROP TABLE IF EXISTS t014__no_supported_cols;
$$
DROP TABLE IF EXISTS t014__one_supported_cols;
$$

CREATE TABLE t014__no_supported_cols (
    arr INT[]
)
$$

CREATE TABLE t014__one_supported_cols (
    str VARCHAR(255),
    arr INT[]
)
