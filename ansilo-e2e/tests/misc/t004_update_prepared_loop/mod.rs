use std::env;

use ansilo_e2e::current_dir;
use itertools::Itertools;
use postgres::types::Type;
use pretty_assertions::assert_eq;
use serial_test::serial;

#[test]
#[serial]
fn test() {
    ansilo_logging::init_for_tests();
    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let query = client
        .prepare_typed(
            r#"
            UPDATE people
            SET name = $2
            WHERE id = $1
            "#,
            &[Type::INT4, Type::TEXT],
        )
        .unwrap();

    let params = vec![
        (1, "New1".to_string()),
        (2, "New2".to_string()),
        (3, "New3".to_string()),
        (4, "New4".to_string()),
        (5, "New5".to_string()),
        (6, "New6".to_string()),
        (7, "New7".to_string()),
        (8, "New8".to_string()),
    ];

    for (id, name) in params.iter() {
        let res = client.execute(&query, &[id, name]).unwrap();
        assert_eq!(res, 1);
    }

    let rows = client
        .query(r#"SELECT id::int4, name FROM people"#, &[])
        .unwrap();

    let rows = rows
        .iter()
        .map(|r| (r.get::<_, i32>("id"), r.get::<_, String>("name")))
        .collect_vec();

    assert_eq!(rows, params);
}
