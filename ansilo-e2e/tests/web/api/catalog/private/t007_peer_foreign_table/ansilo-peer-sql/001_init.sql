CREATE VIEW people
AS SELECT
    'Elizabeth' as name,
    20 as age;

COMMENT ON COLUMN people.name IS 'This is the name of the person';