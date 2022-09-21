CREATE TABLE build (
    file TEXT,
    usr TEXT
);

INSERT INTO build VALUES ('001_stage', current_user);

GRANT SELECT ON build TO app;
GRANT INSERT ON build TO svc;
