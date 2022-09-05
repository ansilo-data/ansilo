use std::time::Duration;

use ansilo_e2e::current_dir;
use criterion::Criterion;
use postgres::Client;

pub fn criterion_benchmark(c: &mut Criterion) {
    let containers = ansilo_e2e::oracle::start_oracle();
    let _oracle =
        ansilo_e2e::oracle::init_oracle_sql(&containers, current_dir!().join("oracle-sql/*.sql"));

    let (_instance, mut client) =
        ansilo_e2e::util::main::run_instance(current_dir!().join("config.yml"));

    let mut g = c.benchmark_group("oracle/b002_insert_1e4_rows");
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(15));
    g.throughput(criterion::Throughput::Elements(10_000));
    g.bench_function("bench", |b| b.iter(|| bench(&mut client)));
    g.finish();
}

fn bench(client: &mut Client) {
    let _rows = client
        .query("INSERT INTO \"B002__TEST_TAB\" SELECT generate_series(1, 10000)", &[])
        .unwrap();

    // TODO: Return inserted rows
    // assert_eq!(rows.len(), 100_000);
}
