use std::time::Duration;

use ansilo_e2e::current_dir;
use criterion::Criterion;
use postgres::Client;

pub fn criterion_benchmark(c: &mut Criterion) {
    ansilo_logging::init();
    let mut g = c.benchmark_group("mysql_oracle/b001_copy_mysql_to_oracle");
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(15));
    g.throughput(criterion::Throughput::Elements(100));
    g.bench_function("bench", |b| {
        let oracle = ansilo_e2e::oracle::start_oracle();
        ansilo_e2e::oracle::init_oracle_sql(&oracle, current_dir!().join("oracle-sql/*.sql"));

        let mysql = ansilo_e2e::mysql::start_mysql();
        ansilo_e2e::mysql::init_mysql_sql(&mysql, current_dir!().join("mysql-sql/*.sql"));

        #[allow(unused)]
        let [(mysql_instance, mut mysql_client), (oracle_instance, mut oracle_client)] =
            ansilo_e2e::peer::run_instances([
                ("MYSQL", current_dir!().join("mysql-config.yml")),
                ("ORACLE", current_dir!().join("oracle-config.yml")),
            ]);

        b.iter(|| bench(&mut oracle_client))
    });
    g.finish();
}

fn bench(client: &mut Client) {
    client
        .execute(r#" DELETE FROM "B001__USERS"; "#, &[])
        .unwrap();

    let rows = client
        .execute(
            r#"
        INSERT INTO "B001__USERS"
            SELECT * FROM peer.b001__users;
        "#,
            &[],
        )
        .unwrap();

    assert_eq!(rows, 10000);
}
