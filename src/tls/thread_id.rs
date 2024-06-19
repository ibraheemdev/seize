// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{Mutex, OnceLock};

/// Thread ID manager which allocates thread IDs. It attempts to aggressively
/// reuse thread IDs where possible to avoid cases where a ThreadLocal grows
/// indefinitely when it is used by many short-lived threads.
#[derive(Default)]
struct ThreadIdManager {
    free_from: usize,
    free_list: BinaryHeap<Reverse<usize>>,
}

impl ThreadIdManager {
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
    pub fn current() -> Thread {
        THREAD.with(|holder| holder.0)
    }

    /// Get the current thread.
    pub fn create() -> Thread {
        Thread::new(thread_id_manager().lock().unwrap().alloc())
    }

    pub fn free(self) {
        thread_id_manager().lock().unwrap().free(self.id);
    }
}

/// Wrapper around `Thread` that allocates and deallocates the ID.
struct ThreadGuard(Thread);

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        Thread::free(self.0)
    }
}

thread_local!(static THREAD: ThreadGuard = ThreadGuard(Thread::create()));

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
