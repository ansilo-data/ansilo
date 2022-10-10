use criterion::criterion_main;

pub mod oracle;
pub mod postgres;
pub mod mysql_oracle;

criterion_main!(oracle::group, postgres::group, mysql_oracle::group);
