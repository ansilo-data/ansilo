IMPORT FOREIGN SCHEMA "db.b001__%" 
FROM SERVER mysql INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;

