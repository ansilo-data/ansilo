DROP TABLE IF EXISTS b003__test_tab
$$

CREATE TABLE b003__test_tab (
    x INT
)
$$

INSERT INTO b003__test_tab
    SELECT generate_series(1, 500)
    