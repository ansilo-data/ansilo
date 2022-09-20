CREATE VIEW dummy AS SELECT 'data' as data;
CREATE VIEW private AS SELECT 'secret' as data;

GRANT SELECT ON dummy TO test_user;
