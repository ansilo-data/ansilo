CREATE SCHEMA private;

CREATE TABLE private.people AS
SELECT
    'Mary' AS name,
    20 AS age;

COMMENT ON TABLE private.people IS 'This is the list of people';

GRANT USAGE ON SCHEMA private TO app;
GRANT SELECT ON private.people TO app;
