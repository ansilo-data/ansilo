ALTER SESSION SET CONTAINER = db;
CREATE USER ansilo_admin IDENTIFIED BY "ansilo_testing";
GRANT ALL PRIVILEGES TO ansilo_admin;
ALTER USER ansilo_admin QUOTA UNLIMITED ON USERS;