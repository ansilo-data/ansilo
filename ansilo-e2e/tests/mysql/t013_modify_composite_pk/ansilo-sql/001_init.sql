IMPORT FOREIGN SCHEMA "db.t013__%" 
FROM SERVER mysql INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
