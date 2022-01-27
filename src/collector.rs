use crate::{raw, Slots};

use std::marker::PhantomData;

/// Fast and robust lock-free memory reclamation for Rust.
///
/// See the [guide](crate#guide) for usage details.
pub struct Collector<S: Slots> {
    raw: raw::Collector<S>,
}

impl<S: Slots> Collector<S> {
    const DEFAULT_EPOCH_TICK: u64 = 110;
    const DEFAULT_RETIRE_TICK: usize = 120;

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
    pub fn epoch_frequency(mut self, n: u64) -> Self {
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
    /// less frequently, but retirement also becomes more
    /// expensive due to longer retirement lists needing
    /// to be traversed and freed.
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
    /// See the [usage guide](crate#guide) for details.
    pub fn guard(&self) -> Guard<'_, S> {
        Guard {
            collector: self,
            _not_send: PhantomData,
        }
    }

    /// Link a value to the collector.
    ///
    /// Seize requires an extra node to be allocated
    /// with each value in order to keep track of
    /// it's reclamation status. Because of this, values
    /// in a concurrent datastructure must take the form of
    /// `AtomicPtr<Linked<T>>`, as opposed to `AtomicPtr<T>`.
    ///
    /// See the [usage guide](crate#guide) for details.
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
    /// See the [usage guide](crate#guide) for details.
    pub unsafe fn retire<T>(&self, ptr: *mut Linked<T>, reclaim: unsafe fn(Link)) {
        self.raw.retire(ptr, reclaim)
    }
}

/// A guard that can protect loads of atomic pointers.
///
/// A guard can be created from a [collector](Collector::guard).
/// See the [usage guide](crate#guide) for details.
pub struct Guard<'a, S: Slots> {
    collector: &'a Collector<S>,
    _not_send: PhantomData<*mut ()>,
}

impl<S: Slots> Guard<'_, S> {
    /// Protect the load of an atomic pointer.
    ///
    /// See the [usage guide](crate#guide) for details.
    pub fn protect<T>(&self, op: impl FnMut() -> *mut Linked<T>, protection: S) -> *mut Linked<T> {
        self.collector.raw.protect(op, protection.as_index())
    }
}

impl<S: Slots> Drop for Guard<'_, S> {
    fn drop(&mut self) {
        unsafe { self.collector.raw.clear_all() }
    }
}

/// The link part of a [`Linked<T>`].
///
/// See the [usage guide](crate#guide) for details.
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

/// A value linked to a collector.
///
/// This type implements `Deref` and `DerefMut` to the
/// inner value, so you can access methods on fields
/// on it as normal. An extra `*` may be needed when
/// `T` needs to be accessed directly.
///
/// A value can be linked with the [`Collector::link`] method.
/// See it's documentation for details.
#[repr(C)]
pub struct Linked<T> {
    pub(crate) node: raw::Node, // invariant: this field must come first
    value: T,
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
