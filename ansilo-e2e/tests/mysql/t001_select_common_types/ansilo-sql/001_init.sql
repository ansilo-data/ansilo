CREATE SERVER mysql
FOREIGN DATA WRAPPER ansilo_fdw 
OPTIONS (
    data_source 'mysql'
);

IMPORT FOREIGN SCHEMA "db.t001__test_tab%" 
FROM SERVER mysql INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ansiloapp;
