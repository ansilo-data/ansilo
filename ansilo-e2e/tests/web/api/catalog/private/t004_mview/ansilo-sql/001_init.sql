CREATE SCHEMA private;

CREATE MATERIALIZED VIEW private.people AS
SELECT
    'Mary' AS name,
    20 AS age;

COMMENT ON MATERIALIZED VIEW private.people IS 'This is the list of people';
COMMENT ON COLUMN private.people.age IS 'How old is the person?';

GRANT USAGE ON SCHEMA private TO app;
GRANT SELECT ON private.people TO app;