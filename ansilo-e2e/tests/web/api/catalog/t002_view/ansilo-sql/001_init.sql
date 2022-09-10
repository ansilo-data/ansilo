CREATE VIEW people AS
SELECT
    'Mary' AS name,
    20 AS age,
    true AS happy;

COMMENT ON VIEW people IS 'This is the list of people';
COMMENT ON COLUMN people.name IS 'This person''s name';