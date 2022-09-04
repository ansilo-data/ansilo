CREATE TABLE storage (
    data VARCHAR(255)
);

INSERT INTO storage VALUES ('secret');

GRANT SELECT ON storage TO token_read;
GRANT SELECT, INSERT, UPDATE, DELETE ON storage TO token_maintain;
