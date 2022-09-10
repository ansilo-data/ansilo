CREATE VIEW people
AS SELECT
    'Elizabeth' as name,
    20 as age;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO app;