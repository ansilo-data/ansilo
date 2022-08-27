use criterion::criterion_main;

pub mod oracle;

criterion_main!(oracle::group);
