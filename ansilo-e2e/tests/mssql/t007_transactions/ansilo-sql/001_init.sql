IMPORT FOREIGN SCHEMA "dbo.t007__%" 
FROM SERVER mssql INTO public;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
