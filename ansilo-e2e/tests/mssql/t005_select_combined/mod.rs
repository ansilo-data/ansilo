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
    let containers = ansilo_e2e::mssql::start_mssql();
    ansilo_e2e::mssql::init_mssql_sql(&containers, current_dir!().join("mssql-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h.name, COUNT(*) as pets 
            FROM "t005__people" h
            INNER JOIN "t005__pets" p ON p.owner_id = h.id
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
            "mssql".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT [t1].[name] AS [c0], COUNT_BIG(*) AS [c1] "#,
                    r#"FROM [dbo].[t005__people] AS [t1] "#,
                    r#"INNER JOIN [dbo].[t005__pets] AS [t2] ON (([t1].[id]) = ([t2].[owner_id])) "#,
                    r#"WHERE (([t2].[name]) != (?)) "#,
                    r#"GROUP BY [t1].[name] "#,
                    r#"ORDER BY COUNT_BIG(*) DESC "#,
                    r#"OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY"#,
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
