IMPORT FOREIGN SCHEMA "%"
FROM SERVER peer1_people
INTO public;

IMPORT FOREIGN SCHEMA "%"
FROM SERVER peer2_pets
INTO public;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO app;