IMPORT FOREIGN SCHEMA "ANSILO_ADMIN.B001__%" 
FROM SERVER oracle INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
