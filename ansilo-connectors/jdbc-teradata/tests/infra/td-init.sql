CREATE DATABASE testdb AS PERMANENT = 600e6, SPOOL = 1200e6; 

CREATE USER  ansilo_admin  FROM  testdb  AS PASSWORD =  ansilo_testing  PERM =  300e6  SPOOL =  600e6  TEMPORARY =  1200e6 DEFAULT DATABASE =  testdb;

GRANT ALL PRIVILEGES ON testdb TO ansilo_admin;
