use jni::objects::{JObject, JValue};

use super::Jvm;

pub fn create_sqlite_memory_connection<'a>(jvm: &'a Jvm<'a>) -> JObject<'a> {
    // in theory we should be able to invoke DriverManager.getConnection
    // directly through JNI using the following code:
    // let jdbc_con = env
    //     .call_static_method(
    //         "java/sql/DriverManager",
    //         "getConnection",
    //         "(Ljava/lang/String;)Ljava/sql/Connection;",
    //         &[JValue::Object(
    //             *env.new_string("jdbc:sqlite::memory:").unwrap(),
    //         )],
    //     )
    //     .unwrap()
    //     .l()
    //     .unwrap();
    // However this code complains it cannot find the driver
    // I have not worked out why this fails but calling our wrapper succeeds...

    let env = &jvm.env;
    let url = env.auto_local(env.new_string("jdbc:sqlite::memory:").unwrap());
    let props = env.auto_local(env.new_object("java/util/Properties", "()V", &[]).unwrap());

    let jdbc_con = env.auto_local(
        env.new_object(
            "com/ansilo/connectors/JdbcConnection",
            "(Ljava/lang/String;Ljava/util/Properties;)V",
            &[JValue::Object(url.as_obj()), JValue::Object(props.as_obj())],
        )
        .unwrap(),
    );

    let jdbc_con = env
        .get_field(jdbc_con.as_obj(), "connection", "Ljava/sql/Connection;")
        .unwrap()
        .l()
        .unwrap();

    jdbc_con
}
