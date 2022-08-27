use std::env;

use ansilo_connectors_base::interface::LoggedQuery;
use ansilo_e2e::current_dir;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let containers = ansilo_e2e::oracle::start_oracle();
    ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h."NAME", COUNT(*) as pets 
            FROM "ANSILO_ADMIN.T014__PEOPLE" h
            INNER JOIN "ANSILO_ADMIN.T014__PETS" p ON p."OWNER_ID" = h."ID"
            WHERE p."NAME" != 'XXX'
            GROUP BY h."NAME"
            ORDER BY pets DESC
            LIMIT 3
            "#,
            &[],
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
            .collect_vec(),
        vec![("John".into(), 2), ("Jane".into(), 1),]
    );

    assert_eq!(
        instance.log().get_from_memory().unwrap(),
        vec![(
            "oracle".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."NAME" AS "c0", COUNT(*) AS "c1" "#,
                    r#"FROM "ANSILO_ADMIN"."T014__PEOPLE" "t1" "#,
                    r#"INNER JOIN "ANSILO_ADMIN"."T014__PETS" "t2" ON (("t1"."ID") = ("t2"."OWNER_ID")) "#,
                    r#"WHERE (("t2"."NAME") != (?)) "#,
                    r#"GROUP BY "t1"."NAME" "#,
                    r#"ORDER BY COUNT(*) DESC "#,
                    r#"FETCH FIRST 3 ROWS ONLY"#,
                ]
                .join(""),
                vec![
                    "LoggedParam [index=1, method=setNString, value=XXX]".into()
                ],
                None
            )
        )]
    );
}
