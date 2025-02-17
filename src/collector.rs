use crate::raw::{self, membarrier, Thread};
use crate::{LocalGuard, OwnedGuard};

use std::fmt;

/// A concurrent garbage collector.
///
/// A `Collector` manages the access and retirement of concurrent objects
/// Objects can be safely loaded through *guards*, which can be created using
/// the [`enter`](Collector::enter) or [`enter_owned`](Collector::enter_owned)
/// methods.
///
/// Every instance of a concurrent data structure should typically own its
/// `Collector`. This allows the garbage collection of non-`'static` values, as
/// memory reclamation is guaranteed to run when the `Collector` is dropped.
#[repr(transparent)]
pub struct Collector {
    /// The underlying raw collector instance.
    pub(crate) raw: raw::Collector,
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector {
    /// The default batch size for a new collector.
    const DEFAULT_BATCH_SIZE: usize = 32;

    /// Creates a new collector.
    pub fn new() -> Self {
        // Initialize the `membarrier` module, detecting the presence of
        // operating-system strong barrier APIs.
        membarrier::detect();

        let cpus = std::thread::available_parallelism()
            .map(Into::into)
            .unwrap_or(1);

        // Ensure every batch accumulates at least as many entries
        // as there are threads on the system.
        let batch_size = cpus.max(Self::DEFAULT_BATCH_SIZE);

        Self {
            raw: raw::Collector::new(cpus, batch_size),
        }
    }

    /// Sets the number of objects that must be in a batch before reclamation is
    /// attempted.
    ///
    /// Retired objects are added to thread-local *batches* before starting the
    /// reclamation process. After `batch_size` is hit, the objects are moved to
    /// separate *retirement lists*, where reference counting kicks in and
    /// batches are eventually reclaimed.
    ///
    /// A larger batch size amortizes the cost of retirement. However,
    /// reclamation latency can also grow due to the large number of objects
    /// needed to be freed. Note that reclamation can not be attempted
    /// unless the batch contains at least as many objects as the number of
    /// active threads.
    ///
    /// The default batch size is `32`.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.raw.batch_size = batch_size;
        self
    }

    /// Marks the current thread as active, returning a guard that protects
    /// loads of concurrent objects for its lifetime. The thread will be
    /// marked as inactive when the guard is dropped.
    ///
    /// Note that loads of objects that may be retired must be protected with
    /// the [`Guard::protect`]. See [the
    /// guide](crate::guide#starting-operations) for an introduction to
    /// using guards, or the documentation of [`LocalGuard`] for
    /// more details.
    ///
    /// Note that `enter` is reentrant, and it is legal to create multiple
    /// guards on the same thread. The thread will stay marked as active
    /// until the last guard is dropped.
    ///
    /// [`Guard::protect`]: crate::Guard::protect
    ///
    /// # Performance
    ///
    /// Performance-wise, creating and destroying a `LocalGuard` is about the
    /// same as locking and unlocking an uncontended `Mutex`. Because of
    /// this, guards should be reused across multiple operations if
    /// possible. However, holding a guard prevents the reclamation of any
    /// concurrent objects retired during its lifetime, so there is
    /// a tradeoff between performance and memory usage.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// use seize::Guard;
    /// # let collector = seize::Collector::new();
    ///  
    /// // An atomic object.
    /// let ptr = AtomicPtr::new(Box::into_raw(Box::new(1_usize)));
    ///
    /// {
    ///     // Create a guard that is active for this scope.
    ///     let guard = collector.enter();
    ///
    ///     // Read the object using a protected load.
    ///     let value = guard.protect(&ptr, Ordering::Acquire);
    ///     unsafe { assert_eq!(*value, 1) }
    ///
    ///     // If there are other thread that may retire the object,
    ///     // the pointer is no longer valid after the guard is dropped.
    ///     drop(guard);
    /// }
    /// # unsafe { drop(Box::from_raw(ptr.load(Ordering::Relaxed))) };
    /// ```
    #[inline]
    pub fn enter(&self) -> LocalGuard<'_> {
        LocalGuard::enter(self)
    }

    /// Create an owned guard that protects objects for its lifetime.
    ///
    /// Unlike local guards created with [`enter`](Collector::enter), owned
    /// guards are independent of the current thread, allowing them to
    /// implement `Send` and `Sync`. See the documentation of [`OwnedGuard`]
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
    /// [`Guard::defer_retire`](crate::Guard::defer_retire) if the pointer may
    /// still be accessed by the current thread while the guard is active.
    ///
    /// # Safety
    ///
    /// The retired pointer must no longer be accessible to any thread that
    /// enters after it is removed. It also cannot be accessed by the
    /// current thread after `retire` is called.
    ///
    /// Additionally, the pointer must be valid to pass to the provided
    /// reclaimer, once it is safe to reclaim.
    ///
    /// # Examples
    ///
    /// Common reclaimers are provided by the [`reclaim`](crate::reclaim)
    /// module.
    ///
    /// ```
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// # let collector = seize::Collector::new();
    /// use seize::reclaim;
    ///
    /// // An atomic object.
    /// let ptr = AtomicPtr::new(Box::into_raw(Box::new(1_usize)));
    ///
    /// // Create a guard.
    /// let guard = collector.enter();
    ///
    /// // Store a new value.
    /// let old = ptr.swap(Box::into_raw(Box::new(2_usize)), Ordering::Release);
    ///
    /// // Reclaim the old value.
    /// //
    /// // Safety: The `swap` above made the old value unreachable for any new threads.
    /// // Additionally, the old value was allocated with a `Box`, so `reclaim::boxed`
    /// // is valid.
    /// unsafe { collector.retire(old, reclaim::boxed) };
    /// # unsafe { collector.retire(ptr.load(Ordering::Relaxed), reclaim::boxed) };
    /// ```
    ///
    /// Alternative, a custom reclaimer function can be used.
    ///
    /// ```
    /// use seize::Collector;
    ///
    /// let collector = Collector::new();
    ///
    /// // Allocate a value and immediately retire it.
    /// let value: *mut usize = Box::into_raw(Box::new(1_usize));
    ///
    /// // Safety: The value was never shared.
    /// unsafe {
    ///     collector.retire(value, |ptr: *mut usize, _collector: &Collector| unsafe {
    ///         // Safety: The value was allocated with `Box::new`.
    ///         let value = Box::from_raw(ptr);
    ///         println!("Dropping {value}");
    ///         drop(value);
    ///     });
    /// }
    /// ```
    #[inline]
    pub unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        debug_assert!(!ptr.is_null(), "attempted to retire a null pointer");

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
    /// this method takes a shared reference, as reclaimers to
    /// be run by this thread are allowed to access the collector recursively.
    ///
    /// # Notes
    ///
    /// Note that if reclaimers initialize guards across threads, or initialize
    /// owned guards, objects retired through those guards may not be
    /// reclaimed.
    pub unsafe fn reclaim_all(&self) {
        unsafe { self.raw.reclaim_all() };
    }

    // Create a reference to `Collector` from an underlying `raw::Collector`.
    pub(crate) fn from_raw(raw: &raw::Collector) -> &Collector {
        unsafe { &*(raw as *const raw::Collector as *const Collector) }
    }
}

impl Eq for Collector {}

impl PartialEq for Collector {
    /// Checks if both references point to the same collector.
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.raw.id == other.raw.id
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collector")
            .field("batch_size", &self.raw.batch_size)
            .finish()
    }
}
