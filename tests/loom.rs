#![cfg(loom)]

use seize::Collector;

use loom::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;

#[test]
fn single_thread() {
    loom::model(|| {
        seize::protection! {
            enum Protect {
                All,
            }
        }

        struct Foo(usize, Arc<AtomicBool>);

        impl Drop for Foo {
            fn drop(&mut self) {
                self.1.store(true, Ordering::Release);
            }
        }

        let collector = Arc::new(Collector::new().batch_size(1));

        let dropped = Arc::new(AtomicBool::new(false));

        {
            let zero = AtomicPtr::new(collector.link_boxed(Foo(0, dropped.clone())));

            {
                let guard = collector.guard();
                let _ = guard.protect(|| zero.load(Ordering::Acquire), Protect::All);
            }

            {
                let guard = collector.guard();
                let value = guard.protect(|| zero.load(Ordering::Acquire), Protect::All);
                unsafe { guard.seize(value, seize::boxed::<Foo>) }
            }
        }

        assert_eq!(dropped.load(Ordering::Acquire), true);
    });
}

#[test]
fn two_threads() {
    loom::model(move || {
        seize::protection! {
            enum Protect {
                All,
            }
        }

        struct Foo(usize, Arc<AtomicBool>);

        impl Drop for Foo {
            fn drop(&mut self) {
                self.1.store(true, Ordering::Release);
            }
        }

        let collector = Arc::new(Collector::new().batch_size(1));

        let one_dropped = Arc::new(AtomicBool::new(false));
        let zero_dropped = Arc::new(AtomicBool::new(false));

        {
            let zero = AtomicPtr::new(collector.link_boxed(Foo(0, zero_dropped.clone())));
            let guard = collector.guard();
            let value = guard.protect(|| zero.load(Ordering::Acquire), Protect::All);
            unsafe { guard.seize(value, seize::boxed::<Foo>) }
        }

        let (tx, rx) = loom::sync::mpsc::channel();

        let one = Arc::new(AtomicPtr::new(
            collector.link_boxed(Foo(1, one_dropped.clone())),
        ));

        let h = loom::thread::spawn({
            let foo = one.clone();
            let collector = collector.clone();

            move || {
                let guard = collector.guard();
                let _value = guard.protect(|| foo.load(Ordering::Acquire), Protect::All);
                tx.send(()).unwrap();
                drop(guard);
                tx.send(()).unwrap();
            }
        });

        let _ = rx.recv().unwrap(); // wait for thread to access value
        let guard = collector.guard();
        let value = guard.protect(|| one.load(Ordering::Acquire), Protect::All);
        unsafe { guard.seize(value, seize::boxed::<Foo>) }

        let _ = rx.recv().unwrap(); // wait for thread to drop guard
        h.join().unwrap();

        drop(guard);

        assert_eq!(
            (
                zero_dropped.load(Ordering::Acquire),
                one_dropped.load(Ordering::Acquire)
            ),
            (true, true)
        );
    });
}
