IMPORT FOREIGN SCHEMA "t002__%" 
FROM SERVER sqlite INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;