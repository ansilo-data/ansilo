ansilo_util_pgx_install::install_ansilo_pgx!();

use criterion::criterion_group;

pub mod b001_select_rows;
pub mod b002_insert_rows;
pub mod b003_update_rows;

criterion_group!(
    group,
    b001_select_rows::criterion_benchmark,
    b002_insert_rows::criterion_benchmark,
    b003_update_rows::criterion_benchmark,
);
