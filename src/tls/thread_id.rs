// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

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
    fn new(id: usize) -> Thread {
        let bucket = (usize::BITS as usize) - id.leading_zeros() as usize;
        let bucket_size = 1 << bucket.saturating_sub(1);
        let index = if id != 0 { id ^ bucket_size } else { 0 };

        Thread { id, bucket, index }
    }

    pub fn bucket_size(&self) -> usize {
        1 << self.bucket.saturating_sub(1)
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
    let thread = Thread::new(0);
    assert_eq!(thread.id, 0);
    assert_eq!(thread.bucket, 0);
    assert_eq!(thread.index, 0);

    let thread = Thread::new(1);
    assert_eq!(thread.id, 1);
    assert_eq!(thread.bucket, 1);
    assert_eq!(thread.index, 0);

    let thread = Thread::new(2);
    assert_eq!(thread.id, 2);
    assert_eq!(thread.bucket, 2);
    assert_eq!(thread.index, 0);

    let thread = Thread::new(3);
    assert_eq!(thread.id, 3);
    assert_eq!(thread.bucket, 2);
    assert_eq!(thread.index, 1);

    let thread = Thread::new(19);
    assert_eq!(thread.id, 19);
    assert_eq!(thread.bucket, 5);
    assert_eq!(thread.index, 3);
}
