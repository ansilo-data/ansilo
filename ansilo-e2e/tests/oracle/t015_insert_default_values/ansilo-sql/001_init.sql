CREATE SERVER oracle
FOREIGN DATA WRAPPER ansilo_fdw 
OPTIONS (
    data_source 'oracle'
);

IMPORT FOREIGN SCHEMA "ANSILO_ADMIN.T015__%" 
FROM SERVER oracle INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ansiloapp;
