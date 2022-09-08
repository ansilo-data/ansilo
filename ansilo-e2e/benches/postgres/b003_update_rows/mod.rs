use std::time::Duration;

use ansilo_e2e::current_dir;
use criterion::Criterion;
use postgres::Client;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut g = c.benchmark_group("postgres/b003_update_rows");
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(15));
    g.throughput(criterion::Throughput::Elements(500));
    g.bench_function("bench", |b| {
        let containers = ansilo_e2e::postgres::start_postgres();
        let _postgres = ansilo_e2e::postgres::init_postgres_sql(
            &containers,
            current_dir!().join("postgres-sql/*.sql"),
        );

        let (_instance, mut client) =
            ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

        b.iter(|| bench(&mut client))
    });
    g.finish();
}

fn bench(client: &mut Client) {
    // Use a non-deterministic function so the entire UPDATE cannot be pushed down
    let rows = client
        .execute(
            "UPDATE b003__test_tab SET x = floor(random() * 100)",
            &[],
        )
        .unwrap();

    assert_eq!(rows, 500);
}
