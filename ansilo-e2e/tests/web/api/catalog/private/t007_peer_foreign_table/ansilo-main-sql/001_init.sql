CREATE SCHEMA sources;

IMPORT FOREIGN SCHEMA "%"
FROM SERVER peer
INTO sources;

GRANT USAGE ON SCHEMA sources TO app;
GRANT SELECT ON ALL TABLES IN SCHEMA sources TO app;
