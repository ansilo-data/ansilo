CREATE TABLE jobs
AS SELECT 0 as runs, '' as usr;

GRANT SELECT ON jobs TO app;
GRANT SELECT, UPDATE ON jobs TO svc;
