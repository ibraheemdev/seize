// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use super::{SKIP, SKIP_BUCKET};

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{Mutex, OnceLock};

/// Thread ID manager which allocates thread IDs. It attempts to aggressively
/// reuse thread IDs where possible to avoid cases where a `ThreadLocal` grows
/// indefinitely when it is used by many short-lived threads.
#[derive(Default)]
struct ThreadIdManager {
    free_from: usize,
    free_list: BinaryHeap<Reverse<usize>>,
}

impl ThreadIdManager {
    /// Allocate a new thread ID.
    fn alloc(&mut self) -> usize {
        if let Some(id) = self.free_list.pop() {
            id.0
        } else {
            let id = self.free_from;
            self.free_from = self
                .free_from
                .checked_add(1)
                .expect("Ran out of thread IDs");
            id
        }
    }

    /// Free a thread ID for reuse.
    fn free(&mut self, id: usize) {
        self.free_list.push(Reverse(id));
    }
}

fn thread_id_manager() -> &'static Mutex<ThreadIdManager> {
    static THREAD_ID_MANAGER: OnceLock<Mutex<ThreadIdManager>> = OnceLock::new();
    THREAD_ID_MANAGER.get_or_init(Default::default)
}

/// Data which is unique to the current thread while it is running.
/// A thread ID may be reused after a thread exits.
#[derive(Clone, Copy)]
pub struct Thread {
    pub(crate) id: usize,
    pub(crate) bucket: usize,
    pub(crate) index: usize,
}

impl Thread {
    pub(crate) fn new(id: usize) -> Thread {
        let skipped = id.checked_add(SKIP).expect("exceeded maximum length");
        let bucket = usize::BITS - skipped.leading_zeros();
        let bucket = (bucket as usize) - (SKIP_BUCKET + 1);
        let bucket_size = Thread::bucket_size(bucket);
        let index = skipped ^ bucket_size;

        Thread { id, bucket, index }
    }

    pub fn bucket_size(bucket: usize) -> usize {
        1 << (bucket + SKIP_BUCKET)
    }

    /// Get the current thread.
    #[inline]
    pub fn current() -> Thread {
        THREAD.with(|thread| {
            if let Some(thread) = thread.get() {
                thread
            } else {
                Thread::init_slow(thread)
            }
        })
    }

    /// Slow path for allocating a thread ID.
    #[cold]
    fn init_slow(thread: &Cell<Option<Thread>>) -> Thread {
        let new = Thread::create();
        thread.set(Some(new));
        THREAD_GUARD.with(|guard| guard.id.set(new.id));
        new
    }

    /// Create a new thread.
    pub fn create() -> Thread {
        Thread::new(thread_id_manager().lock().unwrap().alloc())
    }

    /// Free the given thread.
    ///
    /// # Safety
    ///
    /// This function must only be called once on a given thread.
    pub unsafe fn free(id: usize) {
        thread_id_manager().lock().unwrap().free(id);
    }
}

// This is split into 2 thread-local variables so that we can check whether the
// thread is initialized without having to register a thread-local destructor.
//
// This makes the fast path smaller.
thread_local! { static THREAD: Cell<Option<Thread>> = const { Cell::new(None) }; }
thread_local! { static THREAD_GUARD: ThreadGuard = const { ThreadGuard { id: Cell::new(0) } }; }

// Guard to ensure the thread ID is released on thread exit.
struct ThreadGuard {
    // We keep a copy of the thread ID in the `ThreadGuard`: we can't
    // reliably access `THREAD` in our `Drop` impl due to the unpredictable
    // order of TLS destructors.
    id: Cell<usize>,
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        // Release the thread ID. Any further accesses to the thread ID
        // will go through get_slow which will either panic or
        // initialize a new ThreadGuard.
        let _ = THREAD.try_with(|thread| thread.set(None));

        // Safety: We are in `drop` and the current thread uniquely owns this ID.
        unsafe { Thread::free(self.id.get()) };
    }
}

#[test]
fn test_thread() {
    use crate::tls::BUCKETS;

    assert_eq!(Thread::bucket_size(0), 32);
    for i in 0..32 {
        let loc = Thread::new(i);
        assert_eq!(loc.bucket, 0);
        assert_eq!(loc.index, i);
    }

    assert_eq!(Thread::bucket_size(1), 64);
    for i in 33..96 {
        let loc = Thread::new(i);
        assert_eq!(loc.bucket, 1);
        assert_eq!(loc.index, i - 32);
    }

    assert_eq!(Thread::bucket_size(2), 128);
    for i in 96..224 {
        let loc = Thread::new(i);
        assert_eq!(loc.bucket, 2);
        assert_eq!(loc.index, i - 96);
    }

    let max = Thread::new(usize::MAX - SKIP);
    assert_eq!(max.bucket, BUCKETS - 1);
    assert_eq!(max.index, (1 << 63) - 1);
}
