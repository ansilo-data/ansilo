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
    let containers = ansilo_e2e::postgres::start_postgres();
    ansilo_e2e::postgres::init_postgres_sql(&containers, current_dir!().join("postgres-sql/*.sql"));

    let (instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h.name, COUNT(*) as pets 
            FROM "t008__people" h
            INNER JOIN "t008__pets" p ON p.owner_id = h.id
            WHERE p.name != 'XXX'
            GROUP BY h.name
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
            "postgres".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT "t1"."name" AS "c0", count(*) AS "c1" "#,
                    r#"FROM "public"."t008__people" AS "t1" "#,
                    r#"INNER JOIN "public"."t008__pets" AS "t2" ON (("t1"."id") = ("t2"."owner_id")) "#,
                    r#"WHERE (("t2"."name") != ($1)) "#,
                    r#"GROUP BY "t1"."name" "#,
                    r#"ORDER BY count(*) DESC "#,
                    r#"LIMIT 3"#,
                ]
                .join(""),
                vec!["value=Utf8String(\"XXX\") type=text".into()],
                None
            )
        )]
    );
}
