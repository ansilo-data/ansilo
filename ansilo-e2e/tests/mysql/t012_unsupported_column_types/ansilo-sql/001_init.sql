IMPORT FOREIGN SCHEMA "db.t012__%" 
FROM SERVER mysql INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;