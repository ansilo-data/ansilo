IMPORT FOREIGN SCHEMA "%"
FROM SERVER peer
INTO public;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO app;