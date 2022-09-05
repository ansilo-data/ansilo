BEGIN
EXECUTE IMMEDIATE 'DROP TABLE B003__TEST_TAB';
EXCEPTION
WHEN OTHERS THEN NULL;
END;
$$

CREATE TABLE B003__TEST_TAB (
    X NUMBER
)
$$

INSERT INTO B003__TEST_TAB
    SELECT LEVEL n
    FROM DUAL
    CONNECT BY LEVEL <= 10000
    