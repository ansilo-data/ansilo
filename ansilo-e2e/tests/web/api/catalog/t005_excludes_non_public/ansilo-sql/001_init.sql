CREATE SCHEMA internal;

CREATE TABLE internal.people AS
SELECT
    'Mary' AS name,
    20 AS age;

CREATE SCHEMA private;

CREATE VIEW private.people AS
SELECT
    'Mary' AS name,
    20 AS age;

CREATE SCHEMA source;

CREATE MATERIALIZED VIEW source.people AS
SELECT
    'Mary' AS name,
    20 AS age;