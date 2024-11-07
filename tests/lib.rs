use seize::{reclaim, Collector, Deferred, Guard, Linked};

use std::mem::ManuallyDrop;
use std::num::NonZeroU64;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, OnceLock};
use std::thread;

struct DropTrack(Arc<AtomicUsize>);

impl Drop for DropTrack {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct UnsafeSend<T>(T);
unsafe impl<T> Send for UnsafeSend<T> {}

#[test]
fn single_thread() {
    let collector = Arc::new(Collector::new().batch_size(2));
    let dropped = Arc::new(AtomicUsize::new(0));

    // multiple of 2
    let items = cfg::ITEMS & !1;

    for _ in 0..items {
        let zero = AtomicPtr::new(collector.link_boxed(DropTrack(dropped.clone())));

        {
            let guard = collector.enter();
            let _ = guard.protect(&zero, Ordering::Relaxed);
        }

        {
            let guard = collector.enter();
            let value = guard.protect(&zero, Ordering::Acquire);
            unsafe { collector.retire(value, reclaim::boxed::<Linked<DropTrack>>) }
        }
    }

    assert_eq!(dropped.load(Ordering::Relaxed), items);
}

#[test]
fn two_threads() {
    let collector = Arc::new(Collector::new().batch_size(3));

    let a_dropped = Arc::new(AtomicUsize::new(0));
    let b_dropped = Arc::new(AtomicUsize::new(0));

    let (tx, rx) = mpsc::channel();

    let one = Arc::new(AtomicPtr::new(
        collector.link_boxed(DropTrack(a_dropped.clone())),
    ));

    let h = thread::spawn({
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
        let zero = AtomicPtr::new(collector.link_boxed(DropTrack(b_dropped.clone())));
        let guard = collector.enter();
        let value = guard.protect(&zero, Ordering::Acquire);
        unsafe { collector.retire(value, reclaim::boxed::<Linked<DropTrack>>) }
    }

    rx.recv().unwrap(); // wait for thread to access value
    let guard = collector.enter();
    let value = guard.protect(&one, Ordering::Acquire);
    unsafe { collector.retire(value, reclaim::boxed::<Linked<DropTrack>>) }

    rx.recv().unwrap(); // wait for thread to drop guard
    h.join().unwrap();

    drop(guard);

    assert_eq!(
        (
            b_dropped.load(Ordering::Acquire),
            a_dropped.load(Ordering::Acquire)
        ),
        (2, 1)
    );
}

#[test]
fn refresh() {
    let collector = Arc::new(Collector::new().batch_size(3));

    let items = (0..cfg::ITEMS)
        .map(|i| AtomicPtr::new(collector.link_boxed(i)))
        .collect::<Arc<[_]>>();

    let handles = (0..cfg::THREADS)
        .map(|_| {
            thread::spawn({
                let items = items.clone();
                let collector = collector.clone();

                move || {
                    let mut guard = collector.enter();

                    for _ in 0..cfg::ITER {
                        for item in items.iter() {
                            let item = guard.protect(item, Ordering::Acquire);
                            unsafe { assert!(**item < cfg::ITEMS) }
                        }

                        guard.refresh();
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for i in 0..cfg::ITER {
        for item in items.iter() {
            let old = item.swap(collector.link_boxed(i), Ordering::AcqRel);
            unsafe { collector.retire(old, reclaim::boxed::<Linked<usize>>) }
        }
    }

    for handle in handles {
        handle.join().unwrap()
    }

    // cleanup
    for item in items.iter() {
        let old = item.swap(ptr::null_mut(), Ordering::Acquire);
        unsafe { collector.retire(old, reclaim::boxed::<Linked<usize>>) }
    }
}

#[test]
fn recursive_retire() {
    fn collector() -> &'static Collector {
        static COLLECTOR: OnceLock<Collector> = OnceLock::new();
        COLLECTOR.get_or_init(|| Collector::new().batch_size(1))
    }

    struct Recursive {
        _value: usize,
        pointers: Vec<*mut Linked<usize>>,
    }

    let ptr = collector().link_boxed(Recursive {
        _value: 0,
        pointers: (0..cfg::ITEMS).map(|i| collector().link_boxed(i)).collect(),
    });

    unsafe {
        collector().retire(ptr, |link| {
            let value = Box::from_raw(link.cast::<Linked<Recursive>>());
            for pointer in value.value.pointers {
                collector().retire(pointer, reclaim::boxed::<Linked<usize>>);
                let mut guard = collector().enter();
                guard.flush();
                guard.refresh();
                drop(guard);
            }
        });

        collector().enter().flush();
    }
}

#[test]
fn reclaim_all() {
    let collector = Collector::new().batch_size(2);

    for _ in 0..cfg::ITER {
        let dropped = Arc::new(AtomicUsize::new(0));

        let items = (0..cfg::ITEMS)
            .map(|_| AtomicPtr::new(collector.link_boxed(DropTrack(dropped.clone()))))
            .collect::<Vec<_>>();

        for item in items {
            unsafe {
                collector.retire(
                    item.load(Ordering::Relaxed),
                    reclaim::boxed::<Linked<DropTrack>>,
                )
            };
        }

        unsafe { collector.reclaim_all() };
        assert_eq!(dropped.load(Ordering::Relaxed), cfg::ITEMS);
    }
}

#[test]
fn recursive_retire_reclaim_all() {
    struct Recursive {
        _value: usize,
        collector: *mut Collector,
        pointers: Vec<*mut Linked<DropTrack>>,
    }

    unsafe {
        // make sure retire runs in drop, not immediately
        let collector = Box::into_raw(Box::new(Collector::new().batch_size(cfg::ITEMS * 2)));
        let dropped = Arc::new(AtomicUsize::new(0));

        let ptr = (*collector).link_boxed(Recursive {
            _value: 0,
            collector,
            pointers: (0..cfg::ITEMS)
                .map(|_| (*collector).link_boxed(DropTrack(dropped.clone())))
                .collect(),
        });

        (*collector).retire(ptr, |link| {
            let value = Box::from_raw(link.cast::<Linked<Recursive>>());
            let collector = value.value.collector;
            for pointer in value.value.pointers {
                (*collector).retire(pointer, reclaim::boxed::<Linked<DropTrack>>);
            }
        });

        (*collector).reclaim_all();
        assert_eq!(dropped.load(Ordering::Relaxed), cfg::ITEMS);
        let _ = Box::from_raw(collector);
    }
}

#[test]
fn deferred() {
    let collector = Arc::new(Collector::new().batch_size(2));
    let dropped = Arc::new(AtomicUsize::new(0));
    let items = cfg::ITEMS & !1;

    let mut batch = Deferred::new();
    let guard = collector.enter();
    for _ in 0..items {
        let zero = AtomicPtr::new(collector.link_boxed(DropTrack(dropped.clone())));
        let value = guard.protect(&zero, Ordering::Acquire);
        unsafe { batch.defer(value) };
    }

    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    unsafe { batch.retire_all(&collector, reclaim::boxed::<Linked<DropTrack>>) };
    // guard is still active
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard);
    // now everything should be retired
    assert_eq!(dropped.load(Ordering::Relaxed), items);
}

#[test]
fn deferred_concurrent() {
    let collector = Arc::new(Collector::new().batch_size(3));

    let items = (0..cfg::ITEMS)
        .map(|i| AtomicPtr::new(collector.link_boxed(i)))
        .collect::<Arc<[_]>>();

    let handles = (0..cfg::THREADS)
        .map(|_| {
            thread::spawn({
                let items = items.clone();
                let collector = collector.clone();

                move || {
                    let mut guard = collector.enter();

                    for _ in 0..cfg::ITER {
                        for item in items.iter() {
                            let item = guard.protect(item, Ordering::Acquire);
                            unsafe { assert!(**item < cfg::ITEMS) }
                        }

                        guard.refresh();
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for i in 0..cfg::ITER {
        let mut batch = Deferred::new();
        for item in items.iter() {
            let old = item.swap(collector.link_boxed(i), Ordering::AcqRel);
            unsafe { batch.defer(old) };
        }

        // retire all items
        unsafe { batch.retire_all(&collector, reclaim::boxed::<Linked<usize>>) }
    }

    for handle in handles {
        handle.join().unwrap()
    }

    // cleanup
    let mut batch = Deferred::new();
    for item in items.iter() {
        let old = item.swap(ptr::null_mut(), Ordering::Acquire);
        unsafe { batch.defer(old) };
    }
    unsafe { batch.retire_all(&collector, reclaim::boxed::<Linked<usize>>) }
}

#[test]
fn defer_retire() {
    let collector = Collector::new().batch_size(5);
    let dropped = Arc::new(AtomicUsize::new(0));

    let objects: Vec<_> = (0..30)
        .map(|_| collector.link_boxed(DropTrack(dropped.clone())))
        .collect();

    let guard = collector.enter();

    for object in objects {
        unsafe { guard.defer_retire(object, reclaim::boxed::<Linked<DropTrack>>) }
        guard.flush();
    }

    // guard is still active
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard);
    // now the objects should have been dropped
    assert_eq!(dropped.load(Ordering::Relaxed), 30);
}

#[test]
fn reentrant() {
    let collector = Arc::new(Collector::new().batch_size(5).epoch_frequency(None));
    let dropped = Arc::new(AtomicUsize::new(0));

    let objects: UnsafeSend<Vec<_>> = UnsafeSend(
        (0..5)
            .map(|_| collector.link_boxed(DropTrack(dropped.clone())))
            .collect(),
    );

    assert_eq!(dropped.load(Ordering::Relaxed), 0);

    let guard1 = collector.enter();
    let guard2 = collector.enter();
    let guard3 = collector.enter();

    thread::spawn({
        let collector = collector.clone();

        move || {
            let guard = collector.enter();
            for object in { objects }.0 {
                unsafe { guard.defer_retire(object, reclaim::boxed::<Linked<DropTrack>>) }
            }
        }
    })
    .join()
    .unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard1);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard2);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard3);
    assert_eq!(dropped.load(Ordering::Relaxed), 5);

    let dropped = Arc::new(AtomicUsize::new(0));

    let objects: UnsafeSend<Vec<_>> = UnsafeSend(
        (0..5)
            .map(|_| collector.link_boxed(DropTrack(dropped.clone())))
            .collect(),
    );

    assert_eq!(dropped.load(Ordering::Relaxed), 0);

    let mut guard1 = collector.enter();
    let mut guard2 = collector.enter();
    let mut guard3 = collector.enter();

    thread::spawn({
        let collector = collector.clone();

        move || {
            let guard = collector.enter();
            for object in { objects }.0 {
                unsafe { guard.defer_retire(object, reclaim::boxed::<Linked<DropTrack>>) }
            }
        }
    })
    .join()
    .unwrap();

    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    guard1.refresh();
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard1);
    guard2.refresh();
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(guard2);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    guard3.refresh();
    assert_eq!(dropped.load(Ordering::Relaxed), 5);
}

#[test]
fn owned_guard() {
    let collector = Collector::new().batch_size(5);
    let dropped = Arc::new(AtomicUsize::new(0));

    let objects = UnsafeSend(
        (0..5)
            .map(|_| AtomicPtr::new(collector.link_boxed(DropTrack(dropped.clone()))))
            .collect::<Vec<_>>(),
    );

    assert_eq!(dropped.load(Ordering::Relaxed), 0);

    thread::scope(|s| {
        let guard1 = collector.enter_owned();

        let guard2 = collector.enter();
        for object in objects.0.iter() {
            unsafe {
                guard2.defer_retire(
                    object.load(Ordering::Acquire),
                    reclaim::boxed::<Linked<DropTrack>>,
                )
            }
        }

        drop(guard2);

        // guard1 is still active
        assert_eq!(dropped.load(Ordering::Relaxed), 0);

        s.spawn(move || {
            for object in objects.0.iter() {
                let _ = unsafe { &*guard1.protect(object, Ordering::Relaxed) };
            }

            // guard1 is still active
            assert_eq!(dropped.load(Ordering::Relaxed), 0);

            drop(guard1);

            assert_eq!(dropped.load(Ordering::Relaxed), 5);
        });
    });
}

#[test]
fn owned_guard_concurrent() {
    let collector = Collector::new().batch_size(1);
    let dropped = Arc::new(AtomicUsize::new(0));

    let objects = UnsafeSend(
        (0..cfg::THREADS)
            .map(|_| AtomicPtr::new(collector.link_boxed(DropTrack(dropped.clone()))))
            .collect::<Vec<_>>(),
    );

    let guard = collector.enter_owned();
    let barrier = Barrier::new(cfg::THREADS);

    thread::scope(|s| {
        for i in 0..cfg::THREADS {
            let guard = &guard;
            let objects = &objects;
            let dropped = &dropped;
            let barrier = &barrier;

            s.spawn(move || {
                barrier.wait();

                unsafe {
                    guard.defer_retire(
                        objects.0[i].load(Ordering::Acquire),
                        reclaim::boxed::<Linked<DropTrack>>,
                    )
                };

                guard.flush();

                for object in objects.0.iter() {
                    let _ = unsafe { &*guard.protect(object, Ordering::Relaxed) };
                }

                assert_eq!(dropped.load(Ordering::Relaxed), 0);
            });
        }
    });

    drop(guard);
    assert_eq!(dropped.load(Ordering::Relaxed), cfg::THREADS);
}

#[test]
fn belongs_to() {
    let a = Collector::new();
    let b = Collector::new();

    assert!(a.enter().belongs_to(&a));
    assert!(!a.enter().belongs_to(&b));
    assert!(b.enter().belongs_to(&b));
    assert!(!b.enter().belongs_to(&a));
}

#[test]
fn stress() {
    // stress test with operation on a shared stack
    for _ in 0..cfg::ITER {
        let stack = Arc::new(Stack::new(1));

        thread::scope(|s| {
            for i in 0..cfg::ITEMS {
                stack.push(i, &stack.collector.enter());
                stack.pop(&stack.collector.enter());
            }

            for _ in 0..cfg::THREADS {
                s.spawn(|| {
                    for i in 0..cfg::ITEMS {
                        stack.push(i, &stack.collector.enter());
                        stack.pop(&stack.collector.enter());
                    }
                });
            }
        });

        assert!(stack.pop(&stack.collector.enter()).is_none());
        assert!(stack.is_empty());
    }
}

#[test]
fn shared_owned_stress() {
    // all threads sharing an owned guard
    for _ in 0..cfg::ITER {
        let stack = Arc::new(Stack::new(1));
        let guard = &stack.collector.enter_owned();

        thread::scope(|s| {
            for i in 0..cfg::ITEMS {
                stack.push(i, guard);
                stack.pop(guard);
            }

            for _ in 0..cfg::THREADS {
                s.spawn(|| {
                    for i in 0..cfg::ITEMS {
                        stack.push(i, guard);
                        stack.pop(guard);
                    }
                });
            }
        });

        assert!(stack.pop(guard).is_none());
        assert!(stack.is_empty());
    }
}

#[test]
fn owned_stress() {
    // all threads creating an owned guard (this is very unrealistic and stresses
    // tls synchronization)
    for _ in 0..cfg::ITER {
        let stack = Arc::new(Stack::new(1));

        thread::scope(|s| {
            for i in 0..cfg::ITEMS {
                let guard = &stack.collector.enter_owned();
                stack.push(i, guard);
                stack.pop(guard);
            }

            for _ in 0..cfg::THREADS {
                s.spawn(|| {
                    for i in 0..cfg::ITEMS {
                        let guard = &stack.collector.enter_owned();
                        stack.push(i, guard);
                        stack.pop(guard);
                    }
                });
            }
        });

        assert!(stack.pop(&stack.collector.enter_owned()).is_none());
        assert!(stack.is_empty());
    }
}

#[derive(Debug)]
pub struct Stack<T> {
    head: AtomicPtr<Linked<Node<T>>>,
    collector: Collector,
}

#[derive(Debug)]
struct Node<T> {
    data: ManuallyDrop<T>,
    next: *mut Linked<Node<T>>,
}

impl<T> Stack<T> {
    pub fn new(batch_size: usize) -> Stack<T> {
        Stack {
            head: AtomicPtr::new(ptr::null_mut()),
            collector: Collector::new()
                .batch_size(batch_size)
                .epoch_frequency(NonZeroU64::new(1)),
        }
    }

    pub fn push(&self, t: T, guard: &impl Guard) {
        let new = self.collector.link_boxed(Node {
            data: ManuallyDrop::new(t),
            next: ptr::null_mut(),
        });

        loop {
            let head = guard.protect(&self.head, Ordering::Relaxed);
            unsafe { (*new).next = head }

            if self
                .head
                .compare_exchange(head, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn pop(&self, guard: &impl Guard) -> Option<T> {
        loop {
            let head = guard.protect(&self.head, Ordering::Acquire);

            if head.is_null() {
                return None;
            }

            let next = unsafe { (*head).next };

            if self
                .head
                .compare_exchange(head, next, Ordering::Relaxed, Ordering::Relaxed)
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

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        let guard = self.collector.enter();
        while self.pop(&guard).is_some() {}
    }
}

#[cfg(miri)]
mod cfg {
    pub const THREADS: usize = 4;
    pub const ITEMS: usize = 100;
    pub const ITER: usize = 4;
}

#[cfg(not(miri))]
mod cfg {
    pub const THREADS: usize = 32;
    pub const ITEMS: usize = 10_000;
    pub const ITER: usize = 50;
}
