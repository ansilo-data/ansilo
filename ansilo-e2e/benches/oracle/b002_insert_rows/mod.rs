use std::time::Duration;

use ansilo_e2e::current_dir;
use criterion::Criterion;
use postgres::Client;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut g = c.benchmark_group("oracle/b002_insert_rows");
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(15));
    g.throughput(criterion::Throughput::Elements(100_000));
    g.bench_function("bench", |b| {
        let containers = ansilo_e2e::oracle::start_oracle();
        let _oracle = ansilo_e2e::oracle::init_oracle_sql(
            &containers,
            current_dir!().join("oracle-sql/*.sql"),
        );

        let (_instance, mut client) =
            ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

        b.iter(|| bench(&mut client))
    });
    g.finish();
}

fn bench(client: &mut Client) {
    let rows = client
        .execute(
            "INSERT INTO \"B002__TEST_TAB\" SELECT generate_series(1, 100000)",
            &[],
        )
        .unwrap();

    assert_eq!(rows, 100_000);
}
