IMPORT FOREIGN SCHEMA "%"
FROM SERVER memory
INTO public;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO app;