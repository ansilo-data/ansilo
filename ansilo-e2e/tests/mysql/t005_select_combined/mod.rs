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
    let containers = ansilo_e2e::mysql::start_mysql();
    ansilo_e2e::mysql::init_mysql_sql(&containers, current_dir!().join("mysql-sql/*.sql"));

    let (instance, mut client) = ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let rows = client
        .query(
            r#"
            SELECT h.name, COUNT(*) as pets 
            FROM "db.t005__people" h
            INNER JOIN "db.t005__pets" p ON p.owner_id = h.id
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
            "mysql".to_string(),
            LoggedQuery::new(
                [
                    r#"SELECT `t1`.`name` AS `c0`, COUNT(*) AS `c1` "#,
                    r#"FROM `db`.`t005__people` AS `t1` "#,
                    r#"INNER JOIN `db`.`t005__pets` AS `t2` ON ((`t1`.`id`) = (`t2`.`owner_id`)) "#,
                    r#"WHERE ((`t2`.`name`) != (?)) "#,
                    r#"GROUP BY `t1`.`name` "#,
                    r#"ORDER BY COUNT(*) DESC "#,
                    r#"LIMIT 3"#,
                ]
                .join(""),
                vec![
                    "LoggedParam [index=1, method=setString, value=XXX]".into()
                ],
                None
            )
        )]
    );
}
