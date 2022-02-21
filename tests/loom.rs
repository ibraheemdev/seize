#![cfg(loom)]

use loom::sync::atomic::{AtomicPtr, Ordering};
use seize::{reclaim, Collector, Linked};

use std::convert::TryInto;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::Arc;

#[test]
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
                collector: Collector::new()
                    .batch_size(batch_size)
                    .epoch_frequency(Some(2_u64.try_into().unwrap())),
            }
        }

        pub fn push(&self, t: T) {
            let n = self.collector.link_boxed(Node {
                data: ManuallyDrop::new(t),
                next: AtomicPtr::new(ptr::null_mut()),
            });

            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head, Ordering::Acquire);
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
            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head, Ordering::Acquire);

                match unsafe { head.as_ref() } {
                    Some(h) => {
                        let next = guard.protect(&h.next, Ordering::Acquire);

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
            let guard = self.collector.enter();
            guard.protect(&self.head, Ordering::Acquire).is_null()
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
            if x % 10_000 == 0 {
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
