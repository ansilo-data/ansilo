use criterion::criterion_group;

pub mod b001_select_1e5_rows;
pub mod b002_insert_1e4_rows;
pub mod b003_update_1e4_rows;

criterion_group!(
    group,
    b001_select_1e5_rows::criterion_benchmark,
    b002_insert_1e4_rows::criterion_benchmark,
    b003_update_1e4_rows::criterion_benchmark,
);
