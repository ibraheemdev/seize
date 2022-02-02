#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use seize::{reclaim, Collector, Linked};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::Arc;

#[test]
#[serial_test::serial]
fn single_thread() {
    loom::model(|| {
        struct Foo(usize, Arc<AtomicUsize>);

        impl Drop for Foo {
            fn drop(&mut self) {
                self.1.fetch_add(1, Ordering::Release);
            }
        }

        let collector = Arc::new(Collector::new().batch_size(2));

        let dropped = Arc::new(AtomicUsize::new(0));

        for _ in 0..22 {
            let zero = AtomicPtr::new(collector.link_boxed(Foo(0, dropped.clone())));

            {
                let guard = collector.guard();
                let _ = guard.protect(&zero);
            }

            {
                let guard = collector.guard();
                let value = guard.protect(&zero);
                unsafe { collector.retire(value, reclaim::boxed::<Foo>) }
            }
        }

        assert_eq!(dropped.load(Ordering::Acquire), 22);
    });
}

#[test]
#[serial_test::serial]
fn two_threads() {
    loom::model(move || {
        struct Foo(usize, Arc<AtomicBool>);

        impl Drop for Foo {
            fn drop(&mut self) {
                self.1.store(true, Ordering::Release);
            }
        }

        let collector = Arc::new(Collector::new().batch_size(3));

        let one_dropped = Arc::new(AtomicBool::new(false));
        let zero_dropped = Arc::new(AtomicBool::new(false));

        let (tx, rx) = loom::sync::mpsc::channel();

        let one = Arc::new(AtomicPtr::new(
            collector.link_boxed(Foo(1, one_dropped.clone())),
        ));

        let h = loom::thread::spawn({
            let foo = one.clone();
            let collector = collector.clone();

            move || {
                let guard = collector.guard();
                let _value = guard.protect(&foo);
                tx.send(()).unwrap();
                drop(guard);
                tx.send(()).unwrap();
            }
        });

        for _ in 0..2 {
            let zero = AtomicPtr::new(collector.link_boxed(Foo(0, zero_dropped.clone())));
            let guard = collector.guard();
            let value = guard.protect(&zero);
            unsafe { collector.retire(value, reclaim::boxed::<Foo>) }
        }

        let _ = rx.recv().unwrap(); // wait for thread to access value
        let guard = collector.guard();
        let value = guard.protect(&one);
        unsafe { collector.retire(value, reclaim::boxed::<Foo>) }

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

#[test]
#[serial_test::serial]
fn treiber_stack() {
    #[derive(Debug)]
    pub struct TreiberStack<T> {
        head: AtomicPtr<Linked<Node<T>>>,
        collector: Collector,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: ManuallyDrop<T>,
        next: AtomicPtr<Linked<Node<T>>>,
    }

    impl<T> TreiberStack<T> {
        pub fn new(batch_size: usize) -> TreiberStack<T> {
            TreiberStack {
                head: AtomicPtr::new(ptr::null_mut()),
                collector: Collector::new().batch_size(batch_size).epoch_frequency(2),
            }
        }

        pub fn push(&self, t: T) {
            let n = self.collector.link_boxed(Node {
                data: ManuallyDrop::new(t),
                next: AtomicPtr::new(ptr::null_mut()),
            });

            let guard = self.collector.guard();

            loop {
                let head = guard.protect(&self.head);
                unsafe { (*n).next.store(head, Ordering::Relaxed) }

                if self
                    .head
                    .compare_exchange(head, n, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn pop(&self) -> Option<T> {
            let guard = self.collector.guard();

            loop {
                let head = guard.protect(&self.head);

                match unsafe { head.as_ref() } {
                    Some(h) => {
                        let next = guard.protect(&h.next);

                        if self
                            .head
                            .compare_exchange(head, next, Ordering::Relaxed, Ordering::Relaxed)
                            .is_ok()
                        {
                            unsafe {
                                self.collector.retire(head, reclaim::boxed::<Node<T>>);
                                return Some(ManuallyDrop::into_inner(ptr::read(&(*h).data)));
                            }
                        }
                    }
                    None => return None,
                }
            }
        }

        pub fn is_empty(&self) -> bool {
            let guard = self.collector.guard();
            guard.protect(&self.head).is_null()
        }
    }

    impl<T> Drop for TreiberStack<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }

    let a = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    loom::model({
        let a = a.clone();
        move || {
            let x = a.fetch_add(1, Ordering::AcqRel);
            if x % 1000 == 0 {
                println!("{}", x);
            }

            let stack1 = Arc::new(TreiberStack::new(5));
            let stack2 = Arc::clone(&stack1);

            let jh = loom::thread::spawn(move || {
                for i in 0..5 {
                    stack2.push(i);
                    assert!(stack2.pop().is_some());
                }
            });

            for i in 0..5 {
                stack1.push(i);
                assert!(stack1.pop().is_some());
            }

            jh.join().unwrap();
            assert!(stack1.pop().is_none());
            assert!(stack1.is_empty());
        }
    });
}
