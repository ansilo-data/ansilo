CREATE FOREIGN TABLE "test_created_from_pg.avro" (
    "int" INT NOT NULL,
    "long" BIGINT NOT NULL,
    "string" VARCHAR(255) NOT NULL,
    "bool" BOOL NOT NULL,
    "float" FLOAT4 NOT NULL,
    "double" FLOAT8 NOT NULL,
    "bytes" BYTEA NOT NULL,
    "uuid" UUID NOT NULL,
    "date" DATE NOT NULL,
    "time_micros" TIME NOT NULL,
    "timestamp_micros" TIMESTAMP NOT NULL,
    "null" INT
)
SERVER avro;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
