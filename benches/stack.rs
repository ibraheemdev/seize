use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

fn treiber_stack(c: &mut Criterion) {
    c.bench_function("trieber_stack-haphazard", |b| {
        b.iter(|| {
            let stack = Arc::new(haphazard_stack::TreiberStack::new());

            let handles = (0..66)
                .map(|_| {
                    let stack = stack.clone();
                    thread::spawn(move || {
                        for i in 0..1000 {
                            stack.push(i);
                            assert!(stack.pop().is_some());
                        }
                    })
                })
                .collect::<Vec<_>>();

            for i in 0..1000 {
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

    c.bench_function("trieber_stack-seize", |b| {
        b.iter(|| {
            let stack = Arc::new(seize_stack::TreiberStack::new());

            let handles = (0..66)
                .map(|_| {
                    let stack = stack.clone();
                    thread::spawn(move || {
                        for i in 0..1000 {
                            stack.push(i);
                            assert!(stack.pop().is_some());
                        }
                    })
                })
                .collect::<Vec<_>>();

            for i in 0..1000 {
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
    use seize::{reclaim, Collector, Guard, Linked};
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
        next: *mut Linked<Node<T>>,
    }

    impl<T> TreiberStack<T> {
        pub fn new() -> TreiberStack<T> {
            TreiberStack {
                head: AtomicPtr::new(ptr::null_mut()),
                collector: Collector::new().epoch_frequency(None),
            }
        }

        pub fn push(&self, value: T) {
            let node = self.collector.link_boxed(Node {
                data: ManuallyDrop::new(value),
                next: ptr::null_mut(),
            });

            let guard = self.collector.enter();

            loop {
                let head = guard.protect(&self.head, Ordering::Acquire);
                unsafe { (*node).next = head }

                if self
                    .head
                    .compare_exchange(head, node, Ordering::AcqRel, Ordering::Relaxed)
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

                let next = unsafe { (*head).next };

                if self
                    .head
                    .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        let data = ptr::read(&(*head).data);
                        self.collector
                            .retire(head, reclaim::boxed::<Linked<Node<T>>>);
                        return Some(ManuallyDrop::into_inner(data));
                    }
                }
            }
        }

        pub fn is_empty(&self) -> bool {
            let guard = self.collector.enter();
            guard.protect(&self.head, Ordering::Relaxed).is_null()
        }
    }

    impl<T> Drop for TreiberStack<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }
}

mod haphazard_stack {
    use haphazard::{Domain, HazardPointer};
    use std::mem::ManuallyDrop;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, Ordering};

    #[derive(Debug)]
    pub struct TreiberStack<T: 'static> {
        head: AtomicPtr<Node<T>>,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: ManuallyDrop<T>,
        next: *mut Node<T>,
    }

    unsafe impl<T> Send for Node<T> {}
    unsafe impl<T> Sync for Node<T> {}

    impl<T> TreiberStack<T> {
        pub fn new() -> TreiberStack<T> {
            TreiberStack {
                head: AtomicPtr::default(),
            }
        }

        pub fn push(&self, value: T) {
            let node = Box::into_raw(Box::new(Node {
                data: ManuallyDrop::new(value),
                next: ptr::null_mut(),
            }));

            let mut h = HazardPointer::new();

            loop {
                let head = match h.protect_ptr(&self.head) {
                    Some((ptr, _)) => ptr.as_ptr(),
                    None => ptr::null_mut(),
                };

                unsafe { (*node).next = head }

                if self
                    .head
                    .compare_exchange(head, node, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn pop(&self) -> Option<T> {
            let mut h = HazardPointer::new();

            loop {
                let (head, _) = h.protect_ptr(&self.head)?;
                let next = unsafe { head.as_ref().next };

                if self
                    .head
                    .compare_exchange(head.as_ptr(), next, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        let data = ptr::read(&head.as_ref().data);
                        Domain::global().retire_ptr::<_, Box<Node<T>>>(head.as_ptr());
                        return Some(ManuallyDrop::into_inner(data));
                    }
                }
            }
        }

        pub fn is_empty(&self) -> bool {
            let mut h = HazardPointer::new();
            unsafe { h.protect(&self.head) }.is_none()
        }
    }

    impl<T> Drop for TreiberStack<T> {
        fn drop(&mut self) {
            while self.pop().is_some() {}
        }
    }
}
