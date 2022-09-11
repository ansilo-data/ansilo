IMPORT FOREIGN SCHEMA "%"
FROM SERVER memory
INTO public;

-- Grant base query access
GRANT SELECT, INSERT, UPDATE, DELETE ON people TO token;

-- Grant SELECT to read scope
ALTER TABLE people OPTIONS (ADD before_select 'check_read_scope');

CREATE FUNCTION check_read_scope() RETURNS VOID
    RETURN STRICT(ARRAY['read', 'maintain'] && string_to_array(auth_context()->'claims'->'scope'->>0, ' '), 'read scope is required');

-- Grant ALL to maintain scope
ALTER TABLE people OPTIONS (ADD before_modify 'check_maintain_scope');

CREATE FUNCTION check_maintain_scope() RETURNS VOID
    RETURN STRICT('maintain' = ANY(string_to_array(auth_context()->'claims'->'scope'->>0, ' ')), 'maintain scope is required');

