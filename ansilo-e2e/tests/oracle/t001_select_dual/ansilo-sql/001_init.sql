IMPORT FOREIGN SCHEMA "SYS.DUAL" 
FROM SERVER oracle INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
