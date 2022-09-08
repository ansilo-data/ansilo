DROP TABLE IF EXISTS b001__test_tab
$$

CREATE TABLE b001__test_tab (
    x INT
)
$$

INSERT INTO b001__test_tab
    SELECT generate_series(1, 100000);
    