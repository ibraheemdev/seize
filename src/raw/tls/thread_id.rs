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

/// An allocator for thread IDs.
///
/// The allocator attempts to aggressively reuse thread IDs where possible to
/// avoid cases where a `ThreadLocal` grows indefinitely when it is used by many
/// short-lived threads.
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

/// Returns a reference to the global thread ID manager.
fn thread_id_manager() -> &'static Mutex<ThreadIdManager> {
    static THREAD_ID_MANAGER: OnceLock<Mutex<ThreadIdManager>> = OnceLock::new();
    THREAD_ID_MANAGER.get_or_init(Default::default)
}

/// A unique identifier for a slot in a triangular vector, such as
/// `ThreadLocal`.
///
/// A thread ID may be reused after the corresponding thread exits.
#[derive(Clone, Copy)]
pub struct Thread {
    /// A unique identifier for the thread.
    pub id: usize,

    /// The index of the entry in the bucket.
    pub entry: usize,

    /// The index of the bucket.
    pub bucket: usize,
}

/// The number of entries that are skipped from the start of a vector.
///
/// Index calculations assume that buckets are of sizes `[2^0, 2^1, ..., 2^63]`.
/// To skip shorter buckets and avoid unnecessary allocations, the zeroeth entry
/// index is remapped to a larger index (`2^0 + ... + 2^4 = 31`).
const ZERO_ENTRY: usize = 31;

/// The number of buckets that are skipped from the start of a vector.
///
/// This is the index that the zeroeth bucket index is remapped to (currently
/// `5`).
const ZERO_BUCKET: usize = (usize::BITS - ZERO_ENTRY.leading_zeros()) as usize;

/// The number of buckets in a vector.
pub const BUCKETS: usize = (usize::BITS as usize) - ZERO_BUCKET;

/// The maximum index of an element in the vector.
///
/// Note that capacity of the vector is:
/// `2^ZERO_BUCKET + ... + 2^63 = usize::MAX - ZERO_INDEX`.
const MAX_INDEX: usize = usize::MAX - ZERO_ENTRY - 1;

impl Thread {
    /// Returns a `ThreadId` identifier from a generic unique thread ID.
    ///
    /// The ID provided must not exceed `MAX_INDEX`.
    #[inline]
    pub fn new(id: usize) -> Thread {
        if id > MAX_INDEX {
            panic!("exceeded maximum thread count")
        }

        // Offset the ID based on the number of entries we skip at the start of the
        // buckets array.
        let index = id + ZERO_ENTRY;

        // Calculate the bucket index based on ⌊log2(index)⌋.
        let bucket = BUCKETS - ((index + 1).leading_zeros() as usize) - 1;

        // Offset the absolute index by the capacity of the preceding buckets.
        let entry = index - (Thread::bucket_capacity(bucket) - 1);

        Thread { id, bucket, entry }
    }

    /// Returns the capacity of the bucket at the given index.
    #[inline]
    pub fn bucket_capacity(bucket: usize) -> usize {
        1 << (bucket + ZERO_BUCKET)
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
    #[inline(never)]
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
    // We keep a copy of the thread ID in the `ThreadGuard`: we can't reliably access
    // `THREAD` in our `Drop` impl due to the unpredictable order of TLS destructors.
    id: Cell<usize>,
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        // Release the thread ID. Any further accesses to the thread ID will go through
        // get_slow which will either panic or initialize a new ThreadGuard.
        let _ = THREAD.try_with(|thread| thread.set(None));

        // Safety: We are in `drop` and the current thread uniquely owns this ID.
        unsafe { Thread::free(self.id.get()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread() {
        assert_eq!(Thread::bucket_capacity(0), 32);
        for i in 0..32 {
            let thread = Thread::new(i);
            assert_eq!(thread.id, i);
            assert_eq!(thread.bucket, 0);
            assert_eq!(thread.entry, i);
        }

        assert_eq!(Thread::bucket_capacity(1), 64);
        for i in 33..96 {
            let thread = Thread::new(i);
            assert_eq!(thread.id, i);
            assert_eq!(thread.bucket, 1);
            assert_eq!(thread.entry, i - 32);
        }

        assert_eq!(Thread::bucket_capacity(2), 128);
        for i in 96..224 {
            let thread = Thread::new(i);
            assert_eq!(thread.id, i);
            assert_eq!(thread.bucket, 2);
            assert_eq!(thread.entry, i - 96);
        }
    }

    #[test]
    fn max_entries() {
        let mut entries = 0;
        for i in 0..BUCKETS {
            entries += Thread::bucket_capacity(i);
        }
        assert_eq!(entries, MAX_INDEX + 1);

        let max = Thread::new(MAX_INDEX);
        assert_eq!(max.id, MAX_INDEX);
        assert_eq!(max.bucket, BUCKETS - 1);
        assert_eq!(Thread::bucket_capacity(BUCKETS - 1), 1 << (usize::BITS - 1));
        assert_eq!(max.entry, (1 << (usize::BITS - 1)) - 1);
    }
}
