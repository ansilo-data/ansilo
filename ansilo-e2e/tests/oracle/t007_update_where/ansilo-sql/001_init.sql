IMPORT FOREIGN SCHEMA "ANSILO_ADMIN.T007__TEST_TAB" 
FROM SERVER oracle INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
