use loom::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use seize::Collector;
use std::sync::Arc;

#[test]
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
