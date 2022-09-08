use criterion::criterion_main;

pub mod oracle;
pub mod postgres;

criterion_main!(oracle::group, postgres::group);
