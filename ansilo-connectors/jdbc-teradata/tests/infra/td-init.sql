CREATE DATABASE testdb AS PERMANENT = 600e6, SPOOL = 1200e6; 

CREATE USER  ansilo_admin  FROM  testdb  AS PASSWORD =  ansilo_testing  PERM =  300e6  SPOOL =  600e6  TEMPORARY =  1200e6 DEFAULT DATABASE =  testdb;

GRANT ALL PRIVILEGES ON testdb TO ansilo_admin;
GRANT ALL PRIVILEGES ON testdb TO dbc;

REPLACE PROCEDURE testdb.DROP_IF_EXISTS(InDatabaseName VARCHAR(50), InTableName VARCHAR(50)) 
BEGIN
  IF EXISTS(SELECT 1 FROM DBC.Tables WHERE DatabaseName = InDatabaseName AND TableName = InTableName) THEN
    CALL DBC.SysExecSQL('DROP TABLE ' || InDatabaseName || '.' || InTableName);
  END IF;
END;
