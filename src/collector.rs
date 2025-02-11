use crate::raw::{self, membarrier, Thread};
use crate::{LocalGuard, OwnedGuard};

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Fast, efficient, and robust memory reclamation.
///
/// A `Collector` manages the allocation and retirement of concurrent objects.
/// Objects can be safely loaded through *guards*, which can be created using
/// the [`enter`](Collector::enter) or [`enter_owned`](Collector::enter_owned)
/// methods.
pub struct Collector {
    id: usize,
    pub(crate) raw: raw::Collector,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Collector {
    const DEFAULT_BATCH_SIZE: usize = 32;

    /// Creates a new collector.
    pub fn new() -> Self {
        static ID: AtomicUsize = AtomicUsize::new(0);

        membarrier::detect();
        let cpus = std::thread::available_parallelism()
            .map(Into::into)
            .unwrap_or(1);

        let batch_size = cpus.max(Self::DEFAULT_BATCH_SIZE);

        Self {
            raw: raw::Collector::new(cpus, batch_size),
            id: ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Sets the number of values that must be in a batch
    /// before reclamation is attempted.
    ///
    /// Retired values are added to thread-local *batches*
    /// before starting the reclamation process. After
    /// `batch_size` is hit, values are moved to separate
    /// *retirement lists*, where reference counting kicks
    /// in and batches are eventually reclaimed.
    ///
    /// A larger batch size means that deallocation is done
    /// less frequently, but reclamation also becomes more
    /// expensive due to longer retirement lists needing
    /// to be traversed and freed.
    ///
    /// Note that batch sizes should generally be larger
    /// than the number of threads accessing objects.
    ///
    /// The default batch size is `32`.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.raw.batch_size = batch_size;
        self
    }

    /// Marks the current thread as active, returning a guard
    /// that allows protecting loads of concurrent objects. The thread
    /// will be marked as inactive when the guard is dropped.
    ///
    /// See [the guide](crate::guide#starting-operations) for an
    /// introduction to using guards, or the documentation of [`LocalGuard`]
    /// for more details.
    ///
    /// # Performance
    ///
    /// Creating and destroying a guard is about the same as locking and
    /// unlocking an uncontended `Mutex`, performance-wise. Because of this,
    /// guards should be re-used across multiple operations if possible.
    /// However, note that holding a guard prevents the reclamation of any
    /// concurrent objects retired during it's lifetime, so there is a
    /// tradeoff between performance and memory usage.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// # let collector = seize::Collector::new();
    /// use seize::{reclaim, Linked, Guard};
    ///
    /// let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    ///
    /// let guard = collector.enter();
    /// let value = guard.protect(&ptr, Ordering::Acquire);
    /// unsafe { assert_eq!(**value, 1) }
    /// # unsafe { guard.defer_retire(value, reclaim::boxed) };
    /// ```
    ///
    /// Note that `enter` is reentrant, and it is legal to create
    /// multiple guards on the same thread. The thread will stay
    /// marked as active until the last guard is dropped:
    ///
    /// ```rust
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// # let collector = seize::Collector::new();
    /// use seize::{reclaim, Linked, Guard};
    ///
    /// let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    ///
    /// let guard1 = collector.enter();
    /// let guard2 = collector.enter();
    ///
    /// let value = guard2.protect(&ptr, Ordering::Acquire);
    /// drop(guard1);
    /// // the first guard is dropped, but `value`
    /// // is still safe to access as a guard still
    /// // exists
    /// unsafe { assert_eq!(**value, 1) }
    /// # unsafe { guard2.defer_retire(value, reclaim::boxed) };
    /// drop(guard2) // _now_, the thread is marked as inactive
    /// ```
    #[inline]
    pub fn enter(&self) -> LocalGuard<'_> {
        LocalGuard::enter(self)
    }

    /// Create an owned guard that protects objects for it's lifetime.
    ///
    /// Unlike local guards created with [`enter`](Collector::enter),
    /// owned guards are independent of the current thread, allowing
    /// them to implement `Send`. See the documentation of [`OwnedGuard`]
    /// for more details.
    #[inline]
    pub fn enter_owned(&self) -> OwnedGuard<'_> {
        OwnedGuard::enter(self)
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    ///
    /// Note that this method is disconnected from any guards on the current
    /// thread, so the pointer may be reclaimed immediately. Use
    /// [`Guard::defer_retire`](crate::Guard::defer_retire) if the pointer
    /// may still be accessed by the current thread.
    ///
    /// # Safety
    ///
    /// The retired object must no longer be accessible to any thread that
    /// enters after it is removed. It also cannot be accessed by the
    /// current thread after `retire` is called.
    ///
    /// Retiring the same pointer twice can cause **undefined behavior**, even
    /// if the reclaimer doesn't free memory.
    ///
    /// Additionally, the pointer must be valid to access as a [`Link`], per the
    /// [`AsLink`] trait, and the reclaimer passed to `retire` must
    /// correctly free values of type `T`.
    ///
    /// # Examples
    ///
    /// Common reclaimers are provided by the [`reclaim`](crate::reclaim)
    /// module.
    ///
    /// ```
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// # let collector = seize::Collector::new();
    /// use seize::{reclaim, Linked};
    ///
    /// let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    ///
    /// let guard = collector.enter();
    /// // store the new value
    /// let old = ptr.swap(collector.link_boxed(2_usize), Ordering::Release);
    /// // reclaim the old value
    /// // safety: the `swap` above made the old value unreachable for any new threads
    /// unsafe { collector.retire(old, reclaim::boxed) };
    /// # unsafe { collector.retire(ptr.load(Ordering::Relaxed), reclaim::boxed) };
    /// ```
    ///
    /// Alternative, a custom reclaimer function can be used:
    ///
    /// ```
    /// # use seize::{Link, Collector, Linked};
    /// # let collector = Collector::new();
    /// let value = collector.link_boxed(1);
    ///
    /// // safety: the value was never shared
    /// unsafe {
    ///     collector.retire(value, |link: *mut Link| unsafe {
    ///         // safety: the value retired was of type *mut Linked<i32>
    ///         let ptr: *mut Linked<i32> = Link::cast(link);
    ///
    ///         // safety: the value was allocated with `link_boxed`
    ///         let value = Box::from_raw(ptr);
    ///         println!("dropping {}", value);
    ///         drop(value);
    ///     });
    /// }
    /// ```
    #[inline]
    pub unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        // Note that `add` doesn't ever actually reclaim the pointer immediately if
        // the current thread is active. Instead, it adds it to the current thread's
        // reclamation list, but we don't guarantee that publicly.
        unsafe { self.raw.add(ptr, reclaim, Thread::current()) }
    }

    /// Reclaim any values that have been retired.
    ///
    /// This method reclaims any objects that have been retired across *all*
    /// threads. After calling this method, any values that were previous
    /// retired, or retired recursively on the current thread during this
    /// call, will have been reclaimed.
    ///
    /// # Safety
    ///
    /// This function is **extremely unsafe** to call. It is only sound when no
    /// threads are currently active, whether accessing values that have
    /// been retired or accessing the collector through any type of guard.
    /// This is akin to having a unique reference to the collector. However,
    /// this method takes a shared reference, as reclaimers to be run by this
    /// thread are allowed to access the collector recursively.
    ///
    /// # Notes
    ///
    /// Note that if reclaimers initialize guards across threads, or initialize
    /// owned guards, objects retired through those guards may not be
    /// reclaimed.
    pub unsafe fn reclaim_all(&self) {
        unsafe { self.raw.reclaim_all() };
    }
}

impl Eq for Collector {}

impl PartialEq for Collector {
    /// Checks if both references point to the same collector.
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collector")
            .field("batch_size", &self.raw.batch_size)
            .finish()
    }
}
