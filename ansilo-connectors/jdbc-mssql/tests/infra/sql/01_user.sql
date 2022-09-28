CREATE DATABASE testdb;
go

USE testdb;
go

CREATE LOGIN ansilo_admin WITH PASSWORD='Ansilo_testing!', DEFAULT_DATABASE=testdb;
go

CREATE USER ansilo_admin_user FOR LOGIN ansilo_admin WITH DEFAULT_SCHEMA=dbo;
go

ALTER ROLE db_owner ADD MEMBER ansilo_admin_user;
go