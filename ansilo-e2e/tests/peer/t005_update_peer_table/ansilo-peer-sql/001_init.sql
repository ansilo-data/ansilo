CREATE TABLE people (
    id INT,
    name VARCHAR(255)
);

INSERT INTO people (id, name) VALUES (1, 'John');

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
