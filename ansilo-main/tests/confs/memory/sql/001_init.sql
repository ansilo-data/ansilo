CREATE SERVER memory
FOREIGN DATA WRAPPER ansilo_fdw 
OPTIONS (
    data_source 'memory'
);

-- IMPORT FOREIGN SCHEMA "all" FROM SERVER memory INTO public;
