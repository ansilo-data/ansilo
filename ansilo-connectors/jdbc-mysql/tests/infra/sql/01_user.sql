CREATE USER 'ansilo_admin'@'%' IDENTIFIED BY 'ansilo_testing';
GRANT ALL PRIVILEGES ON *.* TO 'ansilo_admin'@'%' WITH GRANT OPTION;
CREATE DATABASE db;