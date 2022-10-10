BEGIN
EXECUTE IMMEDIATE 'DROP TABLE B001__USERS';
EXCEPTION
WHEN OTHERS THEN NULL;
END;
$$

CREATE TABLE "B001__USERS" (
  "ID" NUMBER(20) NOT NULL,
  "CREATEDAT" TIMESTAMP DEFAULT NULL,
  "UPDATEDAT" TIMESTAMP DEFAULT NULL,
  "DELETEDAT" TIMESTAMP DEFAULT NULL,
  "USERNAME" VARCHAR(50) NOT NULL,
  "PASSWORD" VARCHAR(64) NOT NULL,
  "ISACTIVE" NUMBER(1) DEFAULT NULL,
  "FAILCOUNT" NUMBER(20) DEFAULT NULL,
  "LASTLOGIN" TIMESTAMP DEFAULT NULL,
  "ROLEID" NUMBER(20) NOT NULL,
  "NAME" VARCHAR(100) NOT NULL,
  "SURNAME" VARCHAR(100) NOT NULL,
  "COUNTRYID" NUMBER(20) NOT NULL,
  "CITYID" NUMBER(20) DEFAULT NULL,
  "PHONE" VARCHAR(15) DEFAULT NULL,
  "DESCRIPTION" VARCHAR(150) DEFAULT NULL,
  "GENDER" NUMBER(3) DEFAULT NULL,
  "DATEOFBIRTH" TIMESTAMP DEFAULT NULL,
  "AVATARID" NUMBER(20) DEFAULT NULL,
  "STATEID" NUMBER(20) DEFAULT NULL,
  PRIMARY KEY ("ID")
)

