use criterion::criterion_group;

pub mod b001_copy_mysql_to_oracle;

criterion_group!(
    group,
    b001_copy_mysql_to_oracle::criterion_benchmark,
);
