//! Runs in its own process, so the barriers below are the first the process
//! issues: on Linux they are the ones that register `membarrier`.

use seize::{reclaim, Collector};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

struct DropTrack(Arc<AtomicUsize>);

impl Drop for DropTrack {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn first_barriers_issued_concurrently_by_many_threads() {
    const THREADS: usize = 8;
    const ITEMS: usize = 64;

    // Every thread exists and is inside a guard before any barrier is issued.
    let collector = Arc::new(Collector::new().batch_size(1));
    let dropped = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let (collector, dropped, start) = (collector.clone(), dropped.clone(), start.clone());
            thread::spawn(move || {
                let guard = collector.enter();
                start.wait();
                for _ in 0..ITEMS {
                    let value = Box::into_raw(Box::new(DropTrack(dropped.clone())));
                    // Safety: `value` was never shared.
                    unsafe { collector.retire(value, reclaim::boxed) };
                }
                drop(guard);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    drop(Arc::into_inner(collector).unwrap());
    assert_eq!(dropped.load(Ordering::Relaxed), THREADS * ITEMS);
}
