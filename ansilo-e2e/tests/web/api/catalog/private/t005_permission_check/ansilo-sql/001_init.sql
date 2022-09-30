CREATE SCHEMA internal;

CREATE TABLE internal.people AS
SELECT
    'Mary' AS name,
    20 AS age;

CREATE TABLE internal.pets AS
SELECT
    'Luna' AS name,
    4 AS age;

CREATE SCHEMA private;

CREATE VIEW private.people AS
SELECT
    'Mary' AS name,
    20 AS age;

GRANT USAGE ON SCHEMA internal TO app;
GRANT SELECT ON internal.people TO app;
