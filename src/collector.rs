use crate::raw;

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{fmt, ptr};

/// Fast, efficient, and robust memory reclamation.
///
/// See the [crate documentation](crate) for details.
pub struct Collector {
    raw: raw::Collector,
    unique: *mut u8,
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Collector {
    const DEFAULT_RETIRE_TICK: usize = 120;
    const DEFAULT_EPOCH_TICK: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(110) };

    /// Creates a new collector.
    pub fn new() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(Into::into)
            .unwrap_or(1);

        Self {
            raw: raw::Collector::with_threads(
                cpus,
                Self::DEFAULT_EPOCH_TICK,
                Self::DEFAULT_RETIRE_TICK,
            ),
            unique: Box::into_raw(Box::new(0)),
        }
    }

    /// Sets the frequency of epoch advancement.
    ///
    /// Seize uses epochs to protect against stalled threads.
    /// The more frequently the epoch is advanced, the faster
    /// stalled threads can be detected. However, it also means
    /// that threads will have to do work to catch up to the
    /// current epoch more often.
    ///
    /// The default epoch frequency is `110`, meaning that
    /// the epoch will advance after every 110 values are
    /// linked to the collector. Benchmarking has shown that
    /// this is a good tradeoff between throughput and memory
    /// efficiency.
    ///
    /// If `None` is passed epoch tracking, and protection
    /// against stalled threads, will be disabled completely.
    pub fn epoch_frequency(mut self, n: Option<NonZeroU64>) -> Self {
        self.raw.epoch_frequency = n;
        self
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
    /// The default batch size is `120`. Tests have shown that
    /// this makes a good tradeoff between throughput and memory
    /// efficiency.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.raw.batch_size = n;
        self
    }

    /// Marks the current thread as active, returning a guard
    /// that allows protecting loads of atomic pointers. The thread
    /// will be marked as inactive when the guard is dropped.
    ///
    /// See [the guide](crate#starting-operations) for an introduction
    /// to using guards.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// # let collector = seize::Collector::new();
    /// use seize::{reclaim, Linked};
    ///
    /// let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    ///
    /// let guard = collector.enter();
    /// let value = guard.protect(&ptr, Ordering::Acquire);
    /// unsafe { assert_eq!(**value, 1) }
    /// # unsafe { guard.defer_retire(value, reclaim::boxed::<Linked<usize>>) };
    /// ```
    ///
    /// Note that `enter` is reentrant, and it is legal to create
    /// multiple guards on the same thread. The thread will stay
    /// marked as active until the last guard is dropped:
    ///
    /// ```rust
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    /// # let collector = seize::Collector::new();
    /// use seize::{reclaim, Linked};
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
    /// # unsafe { guard2.defer_retire(value, reclaim::boxed::<Linked<usize>>) };
    /// drop(guard2) // _now_, the thread is marked as inactive
    /// ```
    pub fn enter(&self) -> Guard<'_> {
        self.raw.enter();

        Guard {
            collector: self,
            _a: PhantomData,
        }
    }

    /// Link a value to the collector.
    ///
    /// See [the guide](crate#allocating-objects) for details.
    pub fn link(&self) -> Link {
        Link {
            node: UnsafeCell::new(self.raw.node()),
        }
    }

    /// Creates a new `Linked` object with the given value.
    ///
    /// This is equivalent to:
    ///
    /// ```ignore
    /// Linked {
    ///     value,
    ///     link: collector.link()
    /// }
    /// ```
    pub fn link_value<T>(&self, value: T) -> Linked<T> {
        Linked {
            link: self.link(),
            value,
        }
    }

    /// Links a value to the collector and allocates it with `Box`.
    ///
    /// This is equivalent to:
    ///
    /// ```ignore
    /// Box::into_raw(Box::new(Linked {
    ///     value,
    ///     link: collector.link()
    /// }))
    /// ```
    pub fn link_boxed<T>(&self, value: T) -> *mut Linked<T> {
        Box::into_raw(Box::new(Linked {
            link: self.link(),
            value,
        }))
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to it.
    ///
    /// Note that this method is disconnected from any guards on the current thread,
    /// so the pointer may be reclaimed immediately. See [`Guard::defer_retire`] if
    /// the pointer may still be accessed by the current thread.
    ///
    /// See [the guide](crate#retiring-objects) for details.
    ///
    /// # Safety
    ///
    /// The retired object must no longer be accessible to any thread that enters
    /// after it is removed. It also cannot be accessed by the current thread
    /// after `retire` is called.
    ///
    /// Additionally, he reclaimer passed to `retire` must correctly deallocate values of type `T`.
    #[allow(clippy::missing_safety_doc)] // in guide
    pub unsafe fn retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        // note that `add` doesn't actually reclaim the pointer immediately if the
        // current thread is active, it instead adds it to it's reclamation list,
        // but we don't guarantee that publicly.
        unsafe { self.raw.add(ptr, reclaim) }
    }

    /// Returns true if both references point to the same collector.
    pub fn ptr_eq(this: &Collector, other: &Collector) -> bool {
        ptr::eq(this.unique, other.unique)
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.unique);
        }
    }
}

impl Clone for Collector {
    fn clone(&self) -> Self {
        Collector::new()
            .batch_size(self.raw.batch_size)
            .epoch_frequency(self.raw.epoch_frequency)
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Collector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut strukt = f.debug_struct("Collector");

        if self.raw.epoch_frequency.is_some() {
            strukt.field("epoch", &self.raw.epoch.load(Ordering::Acquire));
        }

        strukt
            .field("batch_size", &self.raw.batch_size)
            .field("epoch_frequency", &self.raw.epoch_frequency)
            .finish()
    }
}

/// A guard that keeps the current thread marked as active,
/// enabling protected loads of atomic pointers.
///
/// See [`Collector::enter`] for details.
pub struct Guard<'a> {
    collector: *const Collector,
    _a: PhantomData<&'a Collector>,
}

impl Guard<'_> {
    /// Returns a dummy guard.
    ///
    /// Calling [`protect`](Guard::protect) on an unprotected guard will
    /// load the pointer directly, and [`retire`](Guard::defer_retire) will
    /// reclaim objects immediately.
    ///
    /// Unprotected guards are useful when calling guarded functions
    /// on a data structure that has just been created or is about
    /// to be destroyed, because you know that no other thread holds
    /// a reference to it.
    ///
    /// # Safety
    ///
    /// You must ensure that code used with this guard is sound with
    /// the unprotected behavior described above.
    pub const unsafe fn unprotected() -> Guard<'static> {
        Guard {
            collector: ptr::null(),
            _a: PhantomData,
        }
    }

    /// Protects the load of an atomic pointer.
    ///
    /// See [the guide](crate#protecting-pointers) for details.
    #[inline]
    pub fn protect<T: AsLink>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        if self.collector.is_null() {
            // unprotected guard
            return ptr.load(ordering);
        }

        unsafe { (*self.collector).raw.protect(ptr, ordering) }
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to it.
    ///
    /// This method delays reclamation until the guard is dropped as opposed to
    /// [`Collector::retire`], which may reclaim objects immediately.
    ///
    /// See [the guide](crate#retiring-objects) for details.
    #[allow(clippy::missing_safety_doc)] // in guide
    pub unsafe fn defer_retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        if self.collector.is_null() {
            // unprotected guard
            return unsafe { (reclaim)(ptr.cast::<Link>()) };
        }

        unsafe { (*self.collector).raw.add(ptr, reclaim) }
    }

    /// Get a reference to the collector this guard we created from.
    ///
    /// This method is useful when you need to ensure that all guards
    /// used with a data structure come from the same collector.
    ///
    /// If this is an [`unprotected`](Guard::unprotected) guard
    /// this method will return `None`.
    pub fn collector(&self) -> Option<&Collector> {
        unsafe { self.collector.as_ref() }
    }

    /// Refreshes the guard.
    ///
    /// Refreshing a guard is similar to dropping and immediately
    /// creating a new guard. The curent thread remains active, but any
    /// pointers that were previously protected may be reclaimed.
    ///
    /// # Safety
    ///
    /// This method is not marked as `unsafe`, but will affect
    /// the validity of pointers returned by [`protect`](Guard::protect),
    /// similar to dropping a guard. It is intended to be used safely
    /// by users of concurrent data structures, as references will
    /// be tied to the guard and this method takes `&mut self`.
    ///
    /// If this is an [`unprotected`](Guard::unprotected) guard
    /// this method will be a no-op.
    pub fn refresh(&mut self) {
        if self.collector.is_null() {
            return;
        }

        unsafe { (*self.collector).raw.refresh() }
    }

    /// Flush any retired values in the local batch.
    ///
    /// This method flushes any values from the current thread's local
    /// batch, starting the reclamation process. Note that no memory
    /// can be reclaimed while this guard is active, but calling `flush`
    /// may allow memory to be reclaimed more quickly after the guard is
    /// dropped.
    ///
    /// See [`Collector::batch_size`] for details about batching.
    pub fn flush(&self) {
        if self.collector.is_null() {
            return;
        }

        unsafe { (*self.collector).raw.try_retire_batch() }
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if self.collector.is_null() {
            return;
        }

        unsafe { (*self.collector).raw.leave() }
    }
}

impl fmt::Debug for Guard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Guard").finish()
    }
}

/// A link to the collector.
///
/// See [the guide](crate#custom-reclaimers) for details.
#[repr(transparent)]
pub struct Link {
    #[allow(dead_code)]
    pub(crate) node: UnsafeCell<raw::Node>,
}

impl Link {
    /// Cast this `link` to it's underlying type.
    ///
    /// Note that while this function is safe, using the returned
    /// pointer is only sound if the link is in fact a type-erased `T`.
    /// This means that when casting a link in a reclaimer, the value
    /// that was retired must be of type `T`.
    pub fn cast<T: AsLink>(link: *mut Link) -> *mut T {
        link.cast()
    }
}

/// A type that can be pointer-cast to and from a [`Link`].
///
/// Most reclamation use cases can avoid this trait and work instead
/// with the [`Linked`] wrapper type. However, if you want more control
/// over the layout of your type (i.e. are working with a DST),
/// you may need to implement this trait directly.
///
/// # Safety
///
/// Types implementing this trait must be marked `#[repr(C)]`
/// and have a [`Link`] as their **first** field.
///
/// # Examples
///
/// ```rust
/// use seize::{AsLink, Collector, Link};
///
/// #[repr(C)]
/// struct Bytes {
///     // safety invariant: Link must be the first field
///     link: Link,
///     values: [*mut u8; 0],
/// }
///
/// // Safety: Bytes is repr(C) and has Link as it's first field
/// unsafe impl AsLink for Bytes {}
///
/// // Deallocate an `Bytes`.
/// unsafe fn dealloc(ptr: *mut Bytes, collector: &Collector) {
///     collector.retire(ptr, |link| {
///         // safety `ptr` is of type *mut Bytes
///         let link: *mut Bytes = Link::cast(link);
///         // ..
///     });
/// }
/// ```
pub unsafe trait AsLink {}

/// A value [linked](Collector::link) to a collector.
///
/// This type implements `Deref` and `DerefMut` to the
/// inner value, so you can access methods on fields
/// on it as normal. An extra `*` may be needed when
/// `T` needs to be accessed directly.
///
/// See [the guide](crate#allocating-objects) for details.
#[repr(C)]
pub struct Linked<T> {
    pub link: Link, // Safety Invariant: this field must come first
    pub value: T,
}

unsafe impl<T> AsLink for Linked<T> {}

impl<T: PartialEq> PartialEq for Linked<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Linked<T> {}

impl<T: fmt::Debug> fmt::Debug for Linked<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}

impl<T: fmt::Display> fmt::Display for Linked<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl<T> std::ops::Deref for Linked<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Linked<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
