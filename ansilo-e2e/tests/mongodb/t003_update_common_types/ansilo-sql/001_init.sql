IMPORT FOREIGN SCHEMA "db.t003__*" 
FROM SERVER mongodb INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
