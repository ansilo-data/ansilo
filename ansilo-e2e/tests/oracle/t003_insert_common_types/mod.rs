use std::env;

use ansilo_connectors_base::interface::{LoggedQuery, ResultSet};
use ansilo_core::err::Result;

#[test]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = super::common::start_oracle();
    let mut oracle =
        super::common::init_oracle_sql(&containers, crate::current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) =
        crate::common::run_instance(crate::current_dir!().join("config.yml"));

    let rows = client
        .execute(
            r#"
            INSERT INTO "ANSILO_ADMIN.T003__TEST_TAB" (
                "COL_CHAR",
                "COL_NCHAR",
                "COL_VARCHAR2",
                "COL_NVARCHAR2",
                "COL_NUMBER",
                "COL_FLOAT",
                "COL_BINARY_FLOAT",
                "COL_BINARY_DOUBLE",
                "COL_RAW",
                "COL_LONG_RAW",
                "COL_BLOB",
                "COL_CLOB",
                "COL_NCLOB",
                "COL_JSON",
                "COL_DATE",
                "COL_TIMESTAMP",
                "COL_TIMESTAMP_TZ",
                "COL_TIMESTAMP_LTZ",
                "COL_NULL"
            ) VALUES (
                'A',
                '🔥',
                'foobar',
                '🚀',
                123.456,
                567.89,
                11.22,
                33.44,
                'RAW'::bytea,
                'LONG RAW'::bytea,
                'BLOB'::bytea,
                'CLOB',
                '🥑NCLOB',
                '{"foo": "bar"}',
                DATE '2020-12-23',
                TIMESTAMP '2018-02-01 01:02:03',
                TIMESTAMP WITH TIME ZONE '1999-01-15 11:00:00 -5:00',
                TIMESTAMP WITH TIME ZONE '1997-01-31 09:26:56.888 +02:00',
                NULL
            )
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 1);

    // Check data received on oracle end
    let results = oracle
        .execute("SELECT * FROM T003__TEST_TAB", vec![])
        .unwrap()
        .reader()
        .unwrap()
        .iter_rows()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(results, vec![]);

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::query(
                r#"SELECT "t1"."COL_CHAR" AS "c0", "t1"."COL_NCHAR" AS "c1", "t1"."COL_VARCHAR2" AS "c2", "t1"."COL_NVARCHAR2" AS "c3", "t1"."COL_NUMBER" AS "c4", "t1"."COL_FLOAT" AS "c5", "t1"."COL_BINARY_FLOAT" AS "c6", "t1"."COL_BINARY_DOUBLE" AS "c7", "t1"."COL_RAW" AS "c8", "t1"."COL_LONG_RAW" AS "c9", "t1"."COL_BLOB" AS "c10", "t1"."COL_CLOB" AS "c11", "t1"."COL_NCLOB" AS "c12", "t1"."COL_JSON" AS "c13", "t1"."COL_DATE" AS "c14", "t1"."COL_TIMESTAMP" AS "c15", "t1"."COL_TIMESTAMP_TZ" AS "c16", "t1"."COL_TIMESTAMP_LTZ" AS "c17", "t1"."COL_NULL" AS "c18" FROM "ANSILO_ADMIN"."T002__TEST_TAB" "t1""#
            )
        )]
    );
}
