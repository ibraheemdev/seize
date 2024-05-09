use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

fn enter_leave(c: &mut Criterion) {
    let mut group = c.benchmark_group("enter_leave");
    group.bench_function("seize", |b| {
        let collector = seize::Collector::new();
        b.iter(|| {
            black_box(collector.enter());
        });
    });

    group.bench_function("crossbeam", |b| {
        b.iter(|| {
            black_box(crossbeam_epoch::pin());
        });
    });
}

criterion_group!(benches, enter_leave);
criterion_main!(benches);
