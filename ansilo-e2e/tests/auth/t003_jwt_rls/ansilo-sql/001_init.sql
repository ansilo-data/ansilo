CREATE TABLE storage (
    data VARCHAR(255)
);

INSERT INTO storage VALUES ('secret');

ALTER TABLE storage ENABLE ROW LEVEL SECURITY;

-- Grant base query access
GRANT SELECT, INSERT, UPDATE, DELETE ON storage TO token;

-- Grant SELECT to read scope
CREATE POLICY token_read_scope ON storage
FOR SELECT TO token
USING ('read' = ANY(string_to_array(auth_context()->'claims'->'scope'->>0, ' ')));

-- Grant ALL to maintain scope
CREATE POLICY token_maintain_scope ON storage
FOR ALL TO token
USING ('maintain' = ANY(string_to_array(auth_context()->'claims'->'scope'->>0, ' ')));
