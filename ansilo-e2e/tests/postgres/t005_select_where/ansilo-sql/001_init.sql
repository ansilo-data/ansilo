IMPORT FOREIGN SCHEMA "public.t005__%" 
FROM SERVER postgres INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
