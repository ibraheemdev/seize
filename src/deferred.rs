use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crate::raw::Node;
use crate::tls::Thread;
use crate::{AsLink, Collector, Link};

/// A batch of pointers to be reclaimed in the future.
///
/// Sometimes it is necessary to defer the retirement of a batch of pointers.
/// For example, a set of pointers may be reachable from multiple locations in a
/// data structure and can only be retired after a specific object is reclaimed.
/// In such cases, the [`Deferred`] type can serve as a cheap place to defer the
/// retirement of pointers, without allocating extra memory.
///
/// [`Deferred`] is a concurrent list, meaning that pointers can be added from
/// multiple threads concurrently. It is not meant to be used to amortize the
/// cost of retirement, which is done through thread-local batches controlled
/// with [`Collector::batch_size`], as access from a single-thread can be more
/// expensive than is required. Deferred batches are useful when you need to
/// control when a batch of objects is retired directly, a relatively rare use
/// case.
///
/// # Examples
///
/// ```rust
/// # use seize::{Deferred, Collector, Linked, reclaim};
/// # use std::sync::{Arc, atomic::{AtomicPtr, Ordering}};
/// let collector = Collector::new().batch_size(10);
///
/// // allocate a set of pointers
/// let items = (0..10)
///     .map(|i| AtomicPtr::new(collector.link_boxed(i)))
///     .collect::<Arc<[_]>>();
///
/// // create a batch of objects to retire
/// let mut batch = Deferred::new();
///
/// for item in items.iter() {
///     // make the item unreachable with an atomic swap
///     let old = item.swap(std::ptr::null_mut(), Ordering::AcqRel);
///     // don't retire just yet, add the object to the batch
///     unsafe { batch.defer(old) };
/// }
///
/// // sometime later... retire all the items in the batch
/// unsafe { batch.retire_all(&collector, reclaim::boxed::<Linked<usize>>) }
/// ```
#[derive(Default)]
pub struct Deferred {
    head: AtomicPtr<Node>,
    pub(crate) min_epoch: AtomicU64,
}

impl Deferred {
    /// Create a new batch of deferred objects.
    pub const fn new() -> Deferred {
        Deferred {
            head: AtomicPtr::new(ptr::null_mut()),
            min_epoch: AtomicU64::new(0),
        }
    }

    /// Add an object to the batch.
    ///
    /// # Safety
    ///
    /// After this method is called, it is *undefined behavior* to add this
    /// pointer to the batch again, or any other batch. The pointer must
    /// also be valid for access as a [`Link`], per the [`AsLink`] trait.
    pub unsafe fn defer<T: AsLink>(&self, ptr: *mut T) {
        // `ptr` is guaranteed to be a valid pointer that can be cast to a node
        // (because of `T: AsLink`).
        //
        // Any other thread with a reference to the pointer only has a shared reference
        // to the `UnsafeCell<Node>`, which is allowed to alias. The caller guarantees
        // that the same pointer is not retired twice, so we can safely write to the
        // node through the shared pointer.
        let node = UnsafeCell::raw_get(ptr.cast::<UnsafeCell<Node>>());

        let birth_epoch = unsafe { (*node).birth_epoch };

        // Keep track of the oldest node in the batch.
        self.min_epoch.fetch_min(birth_epoch, Ordering::Relaxed);

        // Relaxed: `self.head` is only ever accessed through a mutable reference.
        let mut prev = self.head.load(Ordering::Relaxed);

        loop {
            unsafe { (*node).next_batch = prev }

            // Relaxed: `self.head` is only ever accessed through a mutable reference.
            match self
                .head
                .compare_exchange_weak(prev, node, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(found) => prev = found,
            }
        }
    }

    /// Retires a batch of values, running `reclaim` when no threads hold a
    /// reference to any objects in the batch.
    ///
    /// Note that this method is disconnected from any guards on the current
    /// thread, so the pointers may be reclaimed immediately.
    ///
    /// # Safety
    ///
    /// The safety requirements of [`Collector::retire`] apply to each object in
    /// the batch.
    ///
    /// [`Collector::retire`]: crate::Collector::retire
    pub unsafe fn retire_all(&mut self, collector: &Collector, reclaim: unsafe fn(*mut Link)) {
        // Note that `add_batch` doesn't ever actually reclaim the pointer immediately
        // if the current thread is active, similar to `retire`.
        unsafe { collector.raw.add_batch(self, reclaim, Thread::current()) }
    }

    /// Run a function for each object in the batch.
    ///
    /// This function does not consume the batch and can be called multiple
    /// times, **before retirement**.
    pub fn for_each(&mut self, mut f: impl FnMut(*mut Node)) {
        let mut list = *self.head.get_mut();

        while !list.is_null() {
            let curr = list;

            // Advance the cursor.
            // Safety: `curr` is a valid, non-null node in the list.
            list = unsafe { (*curr).next_batch };

            f(curr);
        }
    }
}
