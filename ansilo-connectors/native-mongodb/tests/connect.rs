mod common;

#[test]
fn test_mongodb_open_connection() {
    let instance = common::start_mongo();
    let con = common::connect_to_mongo(&instance);

    let _ = con.client().list_database_names(None, None).unwrap();
}
