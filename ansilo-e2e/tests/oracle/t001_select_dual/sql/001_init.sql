CREATE SERVER oracle
FOREIGN DATA WRAPPER ansilo_fdw 
OPTIONS (
    data_source 'oracle'
);

-- IMPORT FOREIGN SCHEMA "all" FROM SERVER oracle INTO public;
