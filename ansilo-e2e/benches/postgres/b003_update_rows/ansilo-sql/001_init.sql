IMPORT FOREIGN SCHEMA "public.b003__%" 
FROM SERVER postgres INTO public;

CREATE TABLE b002__test_tab (
    x INT
);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
