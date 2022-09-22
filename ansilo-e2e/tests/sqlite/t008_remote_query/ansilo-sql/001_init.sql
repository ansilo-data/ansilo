IMPORT FOREIGN SCHEMA "t008__%" 
FROM SERVER sqlite INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;

-- Grant access to remote query funcs
GRANT EXECUTE ON FUNCTION remote_query(text, text), remote_query(text, text, variadic "any") TO app;
GRANT EXECUTE ON FUNCTION remote_execute(text, text), remote_execute(text, text, variadic "any") TO app;
