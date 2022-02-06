use crate::raw;

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::{fmt, ptr};

/// Fast, efficient, and robust memory reclamation.
///
/// See the [crate documentation](crate) for details.
pub struct Collector {
    raw: raw::Collector,
}

impl Collector {
    const DEFAULT_RETIRE_TICK: usize = 120;
    const DEFAULT_EPOCH_TICK: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(110) };

    /// Creates a new collector.
    pub fn new() -> Self {
        Self {
            raw: raw::Collector::with_threads(
                num_cpus::get(),
                Self::DEFAULT_EPOCH_TICK,
                Self::DEFAULT_RETIRE_TICK,
            ),
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
    /// before completing their actual retirement. After
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

    /// Returns a guard that can protect loads of atomic pointers.
    ///
    /// See [the guide](crate#beginning-operations) for details.
    pub fn enter(&self) -> Guard<'_> {
        self.raw.enter();

        Guard {
            collector: self,
            should_retire: UnsafeCell::new(false),
            _a: PhantomData,
        }
    }

    /// Link a value to the collector.
    ///
    /// See [the guide](crate#allocating-objects) for details.
    pub fn link<T>(&self, value: T) -> Linked<T> {
        Linked {
            value,
            node: self.raw.node(),
        }
    }

    /// Links a value to the collector and allocates it.
    ///
    /// This is equivalent to:
    ///
    /// ```ignore
    /// Box::into_raw(Box::new(collector.link(value)))
    /// ```
    pub fn link_boxed<T>(&self, value: T) -> *mut Linked<T> {
        Box::into_raw(Box::new(self.link(value)))
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to it.
    ///
    /// See [the guide](crate#retiring-objects) for details.
    #[allow(clippy::missing_safety_doc)] // in guide
    pub unsafe fn retire<T>(&self, ptr: *mut Linked<T>, reclaim: unsafe fn(Link)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        let (should_retire, batch) = self.raw.delayed_retire(ptr, reclaim);

        if should_retire {
            self.raw.retire(batch);
        }
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

/// A guard that can protect loads of atomic pointers.
///
/// A guard can be created from a [collector](Collector::enter).
///
/// See [the guide](crate#beginning-operations) for details.
pub struct Guard<'a> {
    collector: *const Collector,
    should_retire: UnsafeCell<bool>,
    _a: PhantomData<&'a Collector>,
}

impl Guard<'_> {
    /// Returns a dummy guard.
    ///
    /// Calling [`protect`](Guard::protect) on an unprotected guard will
    /// load the pointer directly, and [`retire`](Guard::retire) will
    /// reclaim objects immediately.
    ///
    /// Unprotected guards are useful when calling guarded functions
    /// on a data structure that has just been created or is about
    /// to be destroyed, because you know that know other thread holds
    /// a reference to it.
    pub unsafe fn unprotected() -> Guard<'static> {
        Guard {
            collector: ptr::null(),
            should_retire: UnsafeCell::new(false),
            _a: PhantomData,
        }
    }

    /// Protect the load of an atomic pointer.
    ///
    /// See [the guide](crate#protecting-pointers) for details.
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>) -> *mut Linked<T> {
        if self.collector.is_null() {
            return ptr.load(Ordering::SeqCst);
        }

        unsafe { (*self.collector).raw.protect(ptr) }
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to it.
    ///
    /// This method delays reclamation until the guard is dropped as opposed to
    /// [`Collector::retire`], which may reclaim objects immediately.
    ///
    /// See [the guide](crate#retiring-objects) for details.
    #[allow(clippy::missing_safety_doc)] // in guide
    pub unsafe fn retire<T>(&self, ptr: *mut Linked<T>, reclaim: unsafe fn(Link)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        if self.collector.is_null() {
            return (reclaim)(Link { node: ptr as _ });
        }

        let (should_retire, _) = (*self.collector).raw.delayed_retire(ptr, reclaim);
        *self.should_retire.get() |= should_retire;
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if self.collector.is_null() {
            return;
        }

        unsafe {
            (*self.collector).raw.leave();

            if *self.should_retire.get() {
                (*self.collector).raw.retire_batch();
            }
        }
    }
}

/// The link part of a [`Linked<T>`].
///
/// See [the guide](crate#3-reclaimers) for details.
pub struct Link {
    pub(crate) node: *mut raw::Node,
}

impl Link {
    /// Extract the underlying value from this link.
    ///
    /// # Safety
    ///
    /// Casting to the incorrect type is **undefined behavior**.
    pub unsafe fn cast<T>(&mut self) -> *mut Linked<T> {
        self.node as *mut _
    }
}

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
    pub(crate) node: raw::Node, // Safety Invariant: this field must come first
    value: T,
}

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

/// A linked atomic pointer.
///
/// This is simply a type alias for `std::AtomicPtr<Linked<T>>`.
pub type AtomicPtr<T> = std::sync::atomic::AtomicPtr<Linked<T>>;
