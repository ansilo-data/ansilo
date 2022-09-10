CREATE MATERIALIZED VIEW people AS
SELECT
    'Mary' AS name,
    20 AS age;

COMMENT ON MATERIALIZED VIEW people IS 'This is the list of people';
COMMENT ON COLUMN people.age IS 'How old is the person?';