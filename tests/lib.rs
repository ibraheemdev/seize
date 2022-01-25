#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use seize::Collector;
use std::sync::{Arc, Barrier};

#[test]
#[cfg(loom)]
fn it_works() {
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

        assert_eq!(dropped.load(std::sync::atomic::Ordering::Acquire), true);
    });
}

#[test]
#[cfg(loom)]
fn it_works2() {
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

        let zero_dropped = Arc::new(AtomicBool::new(false));
        let one_dropped = Arc::new(AtomicBool::new(false));

        let zero = AtomicPtr::new(collector.link_boxed(Foo(0, zero_dropped.clone())));

        let guard = collector.guard();
        let value = guard.protect(|| zero.load(Ordering::Acquire), Protect::All);
        unsafe { guard.seize(value, seize::boxed::<Foo>) }

        println!("{}", unsafe { (*value).0 });

        let barrier = Arc::new(Barrier::new(2));
        let one = Arc::new(AtomicPtr::new(
            collector.link_boxed(Foo(1, one_dropped.clone())),
        ));

        let h = loom::thread::spawn({
            let foo = one.clone();
            let barrier = barrier.clone();
            let collector = collector.clone();

            move || {
                let guard = collector.guard();
                let value = guard.protect(|| foo.load(Ordering::Acquire), Protect::All);
                println!("{}", unsafe { (*value).0 });

                barrier.wait();
                drop(guard);
                barrier.wait();
            }
        });

        barrier.wait(); // wait for thread to access value

        let guard = collector.guard();
        let value = guard.protect(|| one.load(Ordering::Acquire), Protect::All);
        println!("{}", unsafe { (*value).0 });
        unsafe { guard.seize(value, seize::boxed::<Foo>) }

        barrier.wait(); // wait for thread to drop guard
        h.join().unwrap();

        drop(guard);

        assert_eq!(
            (
                zero_dropped.load(std::sync::atomic::Ordering::Acquire),
                one_dropped.load(std::sync::atomic::Ordering::Acquire)
            ),
            (true, true)
        );
    });
}
