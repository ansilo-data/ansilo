IMPORT FOREIGN SCHEMA "testdb.t005__%" 
FROM SERVER teradata INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
