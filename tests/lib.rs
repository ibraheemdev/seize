use seize::{reclaim, Collector, Linked};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
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
                collector: Collector::new().batch_size(batch_size),
            }
        }

        pub fn push(&self, t: T) {
            let n = self.collector.link_boxed(Node {
                data: ManuallyDrop::new(t),
                next: AtomicPtr::new(ptr::null_mut()),
            });

            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head);
                unsafe { (*n).next.store(head, Ordering::SeqCst) }

                if self
                    .head
                    .compare_exchange(head, n, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn pop(&self) -> Option<T> {
            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head);

                match unsafe { head.as_ref() } {
                    Some(h) => {
                        let next = guard.protect(&h.next);

                        if self
                            .head
                            .compare_exchange(head, next, Ordering::SeqCst, Ordering::SeqCst)
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
            let guard = self.collector.enter();
            guard.protect(&self.head).is_null()
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
