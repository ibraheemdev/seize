use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

fn treiber_stack(c: &mut Criterion) {
    c.bench_function("trieber_stack-seize", |b| {
        b.iter(|| {
            let stack = Arc::new(seize_stack::TreiberStack::new());

            let handles = (0..66)
                .map(|_| {
                    let stack = stack.clone();
                    thread::spawn(move || {
                        for i in 0..100 {
                            stack.push(i);
                            assert!(stack.pop().is_some());
                        }
                    })
                })
                .collect::<Vec<_>>();

            for i in 0..100 {
                stack.push(i);
                assert!(stack.pop().is_some());
            }

            for handle in handles {
                handle.join().unwrap();
            }

            assert!(stack.pop().is_none());
            assert!(stack.is_empty());
        })
    });

    c.bench_function("treiber_stack-flize", |b| {
        b.iter(|| {
            let stack = Arc::new(flize_stack::TreiberStack::new());

            let handles = (0..66)
                .map(|_| {
                    let stack = stack.clone();
                    thread::spawn(move || {
                        for i in 0..100 {
                            stack.push(i);
                            assert!(stack.pop().is_some());
                        }
                    })
                })
                .collect::<Vec<_>>();

            for i in 0..100 {
                stack.push(i);
                assert!(stack.pop().is_some());
            }

            for handle in handles {
                handle.join().unwrap();
            }

            assert!(stack.pop().is_none());
            assert!(stack.is_empty());
        })
    });
}

criterion_group!(benches, treiber_stack);
criterion_main!(benches);

mod seize_stack {
    use seize::{reclaim, Collector, Linked};
    use std::mem::ManuallyDrop;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, Ordering};

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
        pub fn new() -> TreiberStack<T> {
            TreiberStack {
                head: AtomicPtr::new(ptr::null_mut()),
                collector: Collector::new(),
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
                                let data = ptr::read(&(*h).data);
                                self.collector.retire(head, reclaim::boxed::<Node<T>>);
                                return Some(ManuallyDrop::into_inner(data));
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
}

mod flize_stack {
    use flize::{Atomic, Collector, NullTag, Shared, Shield};
    use std::mem::ManuallyDrop;
    use std::ptr;
    use std::sync::atomic::Ordering;

    #[derive(Debug)]
    pub struct TreiberStack<T> {
        head: Atomic<Node<T>, NullTag, NullTag, 0, 0>,
        collector: Collector,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: ManuallyDrop<T>,
        next: Atomic<Node<T>, NullTag, NullTag, 0, 0>,
    }

    impl<T> TreiberStack<T> {
        pub fn new() -> TreiberStack<T> {
            TreiberStack {
                head: Atomic::null(),
                collector: Collector::new(),
            }
        }

        pub fn push(&self, t: T) {
            let n = Box::into_raw(Box::new(Node {
                data: ManuallyDrop::new(t),
                next: Atomic::null(),
            }));

            let guard = self.collector.thin_shield();

            loop {
                let head = self.head.load(Ordering::Relaxed, &guard);
                unsafe { (*n).next.store(head, Ordering::Relaxed) }

                if self
                    .head
                    .compare_exchange(
                        head,
                        unsafe { Shared::from_ptr(n) },
                        Ordering::Release,
                        Ordering::Relaxed,
                        &guard,
                    )
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn pop(&self) -> Option<T> {
            let guard = self.collector.thin_shield();

            loop {
                let head = self.head.load(Ordering::Relaxed, &guard);

                match unsafe { head.as_ref() } {
                    Some(h) => {
                        let next = h.next.load(Ordering::Relaxed, &guard);

                        if self
                            .head
                            .compare_exchange(
                                head,
                                next,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                &guard,
                            )
                            .is_ok()
                        {
                            unsafe {
                                let ptr = head.as_ptr();
                                guard.retire(move || {
                                    Box::from_raw(ptr);
                                });
                                return Some(ManuallyDrop::into_inner(ptr::read(&(*h).data)));
                            }
                        }
                    }
                    None => return None,
                }
            }
        }

        pub fn is_empty(&self) -> bool {
            let guard = self.collector.thin_shield();
            self.head.load(Ordering::Acquire, &guard).is_null()
        }
    }

    impl<T> Drop for TreiberStack<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }
}
