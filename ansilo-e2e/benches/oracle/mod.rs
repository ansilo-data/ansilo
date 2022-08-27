use criterion::criterion_group;

pub mod b001_select_1e5_rows;

criterion_group!(group, b001_select_1e5_rows::criterion_benchmark);
