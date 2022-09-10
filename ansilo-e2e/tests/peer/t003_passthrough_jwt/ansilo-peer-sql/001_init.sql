CREATE TABLE people
AS SELECT
    'Elizabeth' as name,
    20 as age;

ALTER TABLE people ENABLE ROW LEVEL SECURITY;

-- Grant base query access
GRANT SELECT, INSERT, UPDATE, DELETE ON people TO token, token2;

-- Grant SELECT to read scope
CREATE POLICY token_read_scope ON people
FOR SELECT TO token, token2
USING (STRICT('read_people' = ANY(string_to_array(auth_context()->'claims'->'scope'->>0, ' ')), 'scope "read_people" is required'));
