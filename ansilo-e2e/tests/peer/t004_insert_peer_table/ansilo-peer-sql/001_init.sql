CREATE TABLE people (
    id INT,
    name VARCHAR(255)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;