IMPORT FOREIGN SCHEMA "t009__%" 
FROM SERVER sqlite INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;


CREATE FUNCTION remote_increment_counter() RETURNS INT SECURITY DEFINER
    RETURN remote_execute('sqlite', 'UPDATE t009__test_tab SET cnt = cnt + 1');

