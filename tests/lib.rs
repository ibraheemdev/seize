use seize::{reclaim, Collector, Linked, SingleSlot};

use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::thread;

#[cfg(miri)]
const THREADS: usize = 30;

#[cfg(miri)]
const ITEMS: usize = 300;

#[cfg(not(miri))]
const THREADS: usize = 60;

#[cfg(not(miri))]
const ITEMS: usize = 30_000;

#[test]
fn stress() {
    #[derive(Debug)]
    pub struct TreiberStack<T> {
        head: AtomicPtr<Linked<Node<T>>>,
        collector: Collector<SingleSlot>,
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

            let guard = self.collector.guard();

            loop {
                let head = guard.protect(&self.head, SingleSlot);
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
                let head = guard.protect(&self.head, SingleSlot);

                match unsafe { head.as_ref() } {
                    Some(h) => {
                        let next = guard.protect(&h.next, SingleSlot);

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
            guard.protect(&self.head, SingleSlot).is_null()
        }
    }

    impl<T> Drop for TreiberStack<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }

    let stack = Arc::new(TreiberStack::new(33));

    let handles = (0..THREADS)
        .map(|_| {
            let stack = stack.clone();
            thread::spawn(move || {
                for i in 0..ITEMS {
                    stack.push(i);
                    assert!(stack.pop().is_some());
                }
            })
        })
        .collect::<Vec<_>>();

    for i in 0..ITEMS {
        stack.push(i);
        assert!(stack.pop().is_some());
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(stack.pop().is_none());
    assert!(stack.is_empty());
}
