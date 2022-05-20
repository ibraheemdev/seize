use criterion::{criterion_group, criterion_main, Criterion};

// this benchmark mainly compares uncontended enter/leave overhead
fn single_thread(c: &mut Criterion) {
    c.bench_function("single_thread-seize", |b| {
        let mut stack = seize_stack::Stack::new();

        b.iter(|| {
            for i in 0..1000 {
                stack.push(i);
                assert!(stack.pop().is_some());
            }

            for i in 0..1000 {
                stack.push(i);
                assert!(stack.pop().is_some());
            }

            assert!(stack.pop().is_none());
            assert!(stack.is_empty());
        });

        drop(stack);
    });

    c.bench_function("single_thread-crossbeam", |b| {
        let mut stack = crossbeam_stack::Stack::new();

        b.iter(|| {
            for i in 0..1000 {
                stack.push(i);
                assert!(stack.pop().is_some());
            }

            for i in 0..1000 {
                stack.push(i);
                assert!(stack.pop().is_some());
            }

            assert!(stack.pop().is_none());
            assert!(stack.is_empty());
        })
    });
}

criterion_group!(benches, single_thread);
criterion_main!(benches);

mod seize_stack {
    use criterion::black_box;
    use seize::{Collector, Linked};
    use std::ptr;

    pub struct Stack {
        head: *mut Linked<Node>,
        collector: Collector,
        alloc: *mut Linked<Node>,
    }

    struct Node {
        data: Option<usize>,
        next: *mut Linked<Node>,
    }

    impl Stack {
        pub fn new() -> Stack {
            let collector = Collector::new().epoch_frequency(None);

            Stack {
                head: ptr::null_mut(),
                alloc: collector.link_boxed(Node {
                    data: Some(1),
                    next: ptr::null_mut(),
                }),
                collector,
            }
        }

        pub fn push(&mut self, _value: usize) {
            let guard = black_box(self.collector.enter());
            self.head = self.alloc;
            drop(guard);
        }

        pub fn pop(&mut self) -> Option<usize> {
            let guard = black_box(self.collector.enter());

            unsafe {
                let head = self.head;

                if head.is_null() {
                    return None;
                }

                self.head = (*head).next;

                guard.retire(head, |mut link| {
                    let head = link.cast::<Node>();
                    assert!(!head.is_null());
                    assert!((*head).data == Some(1));
                });

                (*head).data
            }
        }

        pub fn is_empty(&self) -> bool {
            let _guard = black_box(crossbeam_epoch::pin());
            self.head.is_null()
        }
    }
}

mod crossbeam_stack {
    use criterion::black_box;
    use std::ptr;

    pub struct Stack {
        head: *mut Node,
        alloc: *mut Node,
    }

    struct Node {
        data: Option<usize>,
        next: *mut Node,
    }

    impl Stack {
        pub fn new() -> Stack {
            Stack {
                head: ptr::null_mut(),
                alloc: Box::into_raw(Box::new(Node {
                    data: Some(1),
                    next: ptr::null_mut(),
                })),
            }
        }

        pub fn push(&mut self, _value: usize) {
            let guard = black_box(crossbeam_epoch::pin());
            self.head = self.alloc;
            drop(guard);
        }

        pub fn pop(&mut self) -> Option<usize> {
            let guard = black_box(crossbeam_epoch::pin());

            unsafe {
                let head = self.head;

                if head.is_null() {
                    return None;
                }

                self.head = (*head).next;

                guard.defer_unchecked(move || {
                    assert!(!head.is_null());
                    assert!((*head).data == Some(1));
                });

                (*head).data
            }
        }

        pub fn is_empty(&self) -> bool {
            let _guard = black_box(crossbeam_epoch::pin());
            self.head.is_null()
        }
    }
}
