BEGIN
EXECUTE IMMEDIATE 'DROP TABLE T016__TEST_TAB';
EXCEPTION
WHEN OTHERS THEN NULL;
END;
$$

CREATE TABLE T016__TEST_TAB (
    DATA VARCHAR(255)
)