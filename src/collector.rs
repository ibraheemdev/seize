use crate::tls::Thread;
use crate::{raw, LocalGuard, OwnedGuard};

use std::cell::UnsafeCell;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::{fmt, ptr};

/// Fast, efficient, and robust memory reclamation.
///
/// A `Collector` manages the allocation and retirement of concurrent objects.
/// Objects can be safely loaded through *guards*, which can be created using
/// the [`enter`](Collector::enter) or [`enter_owned`](Collector::enter_owned)
/// methods.
pub struct Collector {
    pub(crate) raw: raw::Collector,
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
            raw: raw::Collector::new(cpus, Self::DEFAULT_EPOCH_TICK, Self::DEFAULT_RETIRE_TICK),
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
    /// that allows protecting loads of concurrent objects. The thread
    /// will be marked as inactive when the guard is dropped.
    ///
    /// See [the guide](crate::guide#starting-operations) for an
    /// introduction to using guards, or the documentation of [`LocalGuard`]
    /// for more details.
    ///
    /// # Performance
    ///
    /// Creating and destroying a guard is about the same as locking and unlocking
    /// an uncontended `Mutex`, performance-wise. Because of this, guards should
    /// be re-used across multiple operations if possible. However, note that holding
    /// a guard prevents the reclamation of any concurrent objects retired during
    /// it's lifetime, so there is a tradeoff between performance and memory usage.
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
    /// # unsafe { guard2.defer_retire(value, reclaim::boxed::<Linked<usize>>) };
    /// drop(guard2) // _now_, the thread is marked as inactive
    /// ```
    pub fn enter(&self) -> LocalGuard<'_> {
        LocalGuard::enter(self)
    }

    /// Create an owned guard that protects objects for it's lifetime.
    ///
    /// Unlike local guards created with [`enter`](Collector::enter),
    /// owned guards are independent of the current thread, allowing
    /// them to implement `Send`. See the documentation of [`OwnedGuard`]
    /// for more details.
    pub fn enter_owned(&self) -> OwnedGuard<'_> {
        OwnedGuard::enter(self)
    }

    /// Create a [`Link`] that can be used to link an object to the collector.
    ///
    /// This method is useful when working with a DST where the [`Linked`] wrapper
    /// cannot be used. See [`AsLink`] for details, or use the [`link_value`](Collector::link_value)
    /// and [`link_boxed`](Collector::link_boxed) helpers.
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
    /// so the pointer may be reclaimed immediately. Use [`Guard::defer_retire`](crate::Guard::defer_retire)
    /// if the pointer may still be accessed by the current thread.
    ///
    /// # Safety
    ///
    /// The retired object must no longer be accessible to any thread that enters
    /// after it is removed. It also cannot be accessed by the current thread
    /// after `retire` is called.
    ///
    /// Retiring the same pointer twice can cause **undefined behavior**, even if the
    /// reclaimer doesn't free memory.
    ///
    /// Additionally, the pointer must be valid to access as a [`Link`], per the [`AsLink`]
    /// trait, and the reclaimer passed to `retire` must correctly free values of type `T`.
    ///
    /// # Examples
    ///
    /// Common reclaimers are provided by the [`reclaim`](crate::reclaim) module.
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
    /// unsafe { collector.retire(old, reclaim::boxed::<Linked<usize>>) };
    /// # unsafe { collector.retire(ptr.load(Ordering::Relaxed), reclaim::boxed::<Linked<usize>>) };
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
    pub unsafe fn retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        // note that `add` doesn't ever actually reclaim the pointer immediately if
        // the current thread is active, it instead adds it to it's reclamation list,
        // but we don't guarantee that publicly.
        unsafe { self.raw.add(ptr, reclaim, Thread::current()) }
    }

    pub(crate) fn ptr_eq(this: &Collector, other: &Collector) -> bool {
        ptr::eq(this.unique, other.unique)
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        let _ = unsafe { Box::from_raw(self.unique) };
    }
}

impl Clone for Collector {
    /// Creates a new, independent collector with the same configuration as this one.
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
        let mut f = f.debug_struct("Collector");

        if self.raw.epoch_frequency.is_some() {
            f.field("epoch", &self.raw.epoch.load(Ordering::Acquire));
        }

        f.field("batch_size", &self.raw.batch_size)
            .field("epoch_frequency", &self.raw.epoch_frequency)
            .finish()
    }
}

/// A link to the collector.
///
/// Functions passed to [`retire`](Collector::retire) are called with a type-erased
/// `Link` pointer. To extract the underlying value from a link,
/// you can call the [`cast`](Link::cast) method.
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
/// Most types can avoid this trait and work instead with the [`Linked`]
/// wrapper type. However, if you want more control over the layout of your
/// type (i.e. are working with a DST), you may need to implement this trait
/// directly.
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

/// A value linked to a collector.
///
/// Objects that may be retired must embed the [`Link`] type. This is a convenience wrapper
/// for linked values that can be created with the [`Collector::link_value`] or
/// [`Collector::link_boxed`] methods.
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
