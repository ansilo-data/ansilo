DROP TABLE IF EXISTS t012__no_supported_cols;
$$
DROP TABLE IF EXISTS t012__one_supported_cols;
$$

CREATE TABLE t012__no_supported_cols (
    geo GEOMETRY
)
$$

CREATE TABLE t012__one_supported_cols (
    str VARCHAR(255),
    geo GEOMETRY
)
