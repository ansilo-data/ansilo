CREATE SCHEMA private;

CREATE VIEW private.people AS
SELECT
    'Mary' AS name,
    20 AS age,
    true AS happy;

COMMENT ON VIEW private.people IS 'This is the list of people';
COMMENT ON COLUMN private.people.name IS 'This person''s name';

GRANT USAGE ON SCHEMA private TO app;
GRANT SELECT ON private.people TO app;