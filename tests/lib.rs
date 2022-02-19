use seize::{reclaim, AtomicPtr, Collector, Guard, Linked};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

#[cfg(miri)]
mod cfg {
    pub const THREADS: usize = 4;
    pub const ITEMS: usize = 200;
    pub const ITER: usize = 2;
}

#[cfg(not(miri))]
mod cfg {
    pub const THREADS: usize = 32;
    pub const ITEMS: usize = 10_000;
    pub const ITER: usize = 100;
}

#[test]
fn stress() {
    #[derive(Debug)]
    pub struct TreiberStack<T> {
        head: AtomicPtr<Node<T>>,
        collector: Collector,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: ManuallyDrop<T>,
        next: AtomicPtr<Node<T>>,
    }

    impl<T> TreiberStack<T> {
        pub fn new(batch_size: usize) -> TreiberStack<T> {
            TreiberStack {
                head: AtomicPtr::new(ptr::null_mut()),
                collector: Collector::new().batch_size(batch_size),
            }
        }

        pub fn push(&self, t: T) {
            let new = self.collector.link_boxed(Node {
                data: ManuallyDrop::new(t),
                next: AtomicPtr::new(ptr::null_mut()),
            });

            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head, Ordering::Acquire);
                unsafe { (*new).next.store(head, Ordering::Relaxed) }

                if self
                    .head
                    .compare_exchange(head, new, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn pop(&self) -> Option<T> {
            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head, Ordering::Acquire);

                if head.is_null() {
                    return None;
                }

                let next = unsafe { guard.protect(&(*head).next, Ordering::Acquire) };

                if self
                    .head
                    .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        let data = ptr::read(&(*head).data);
                        self.collector.retire(head, reclaim::boxed::<Node<T>>);
                        return Some(ManuallyDrop::into_inner(data));
                    }
                }
            }
        }

        pub fn is_empty(&self) -> bool {
            let guard = self.collector.enter();
            guard.protect(&self.head, Ordering::Acquire).is_null()
        }
    }

    impl<T> Drop for TreiberStack<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }

    for _ in 0..cfg::ITER {
        let stack = Arc::new(TreiberStack::new(33));

        let handles = (0..cfg::THREADS)
            .map(|_| {
                let stack = stack.clone();
                thread::spawn(move || {
                    for i in 0..cfg::ITEMS {
                        stack.push(i);
                        stack.pop();
                    }
                })
            })
            .collect::<Vec<_>>();

        for i in 0..cfg::ITEMS {
            stack.push(i);
            stack.pop();
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(stack.pop().is_none());
        assert!(stack.is_empty());
    }
}

#[test]
fn delayed_retire() {
    let collector = Collector::new().batch_size(5);
    let objects: Vec<*mut Linked<usize>> = (0..30).map(|_| collector.link_boxed(42)).collect();
    let linked_references: Vec<&Linked<usize>> = objects.iter().map(|x| unsafe { &**x }).collect();
    let references: Vec<&usize> = objects.iter().map(|x| unsafe { &***x }).collect();

    let guard = collector.enter();

    for object in objects {
        unsafe {
            guard.retire(object, reclaim::boxed::<usize>);
        }
    }

    for x in linked_references {
        assert_eq!(**x, 42);
    }

    for x in references {
        assert_eq!(*x, 42);
    }

    drop(guard);
}

#[test]
fn single_thread() {
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
            let guard = collector.enter();
            let _ = guard.protect(&zero, Ordering::Acquire);
        }

        {
            let guard = collector.enter();
            let value = guard.protect(&zero, Ordering::Acquire);
            unsafe { collector.retire(value, reclaim::boxed::<Foo>) }
        }
    }

    assert_eq!(dropped.load(Ordering::Acquire), 22);
}

#[test]
fn two_threads() {
    struct Foo(usize, Arc<AtomicBool>);

    impl Drop for Foo {
        fn drop(&mut self) {
            self.1.store(true, Ordering::Release);
        }
    }

    let collector = Arc::new(Collector::new().batch_size(3));

    let one_dropped = Arc::new(AtomicBool::new(false));
    let zero_dropped = Arc::new(AtomicBool::new(false));

    let (tx, rx) = std::sync::mpsc::channel();

    let one = Arc::new(AtomicPtr::new(
        collector.link_boxed(Foo(1, one_dropped.clone())),
    ));

    let h = std::thread::spawn({
        let one = one.clone();
        let collector = collector.clone();

        move || {
            let guard = collector.enter();
            let _value = guard.protect(&one, Ordering::Acquire);
            tx.send(()).unwrap();
            drop(guard);
            tx.send(()).unwrap();
        }
    });

    for _ in 0..2 {
        let zero = AtomicPtr::new(collector.link_boxed(Foo(0, zero_dropped.clone())));
        let guard = collector.enter();
        let value = guard.protect(&zero, Ordering::Acquire);
        unsafe { collector.retire(value, reclaim::boxed::<Foo>) }
    }

    let _ = rx.recv().unwrap(); // wait for thread to access value
    let guard = collector.enter();
    let value = guard.protect(&one, Ordering::Acquire);
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
}

#[test]
fn collector_eq() {
    let a = Collector::new();
    let b = Collector::new();
    let unprotected = unsafe { Guard::unprotected() };

    assert!(Collector::ptr_eq(
        a.enter().collector().unwrap(),
        a.enter().collector().unwrap()
    ));
    assert!(Collector::ptr_eq(
        a.enter().collector().unwrap(),
        a.enter().collector().unwrap()
    ));
    assert!(!Collector::ptr_eq(
        a.enter().collector().unwrap(),
        b.enter().collector().unwrap()
    ));
    assert!(unprotected.collector().is_none());
}
