use crate::raw;
use crate::reclaim::Reclaim;

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::{fmt, ptr};

/// Fast, efficient, and robust memory reclamation.
///
/// See the [crate documentation](crate) for an introductory guide.
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
        Self {
            raw: raw::Collector::with_threads(
                num_cpus::get(),
                Self::DEFAULT_EPOCH_TICK,
                Self::DEFAULT_RETIRE_TICK,
            ),
            unique: Box::into_raw(Box::new(0)),
        }
    }

    /// Sets the frequency of epoch advancement.
    ///
    /// Collectors use epochs to protect against stalled threads.
    /// The more frequently the epoch is advanced, the faster
    /// stalled threads can be detected. However, it also means
    /// that threads will have to do more work to catch up to the
    /// current epoch.
    ///
    /// The default epoch tick is 110, meaning that the epoch
    /// will advance after every 110 objects are linked to the collector.
    /// Benchmarking has shown that this is a good tradeoff between
    /// throughput and memory efficiency.
    ///
    /// If `None` is passed to this function, epoch tracking, and protection against
    /// stalled threads, will be disabled completely.
    pub fn epoch_tick(mut self, tick: Option<NonZeroU64>) -> Self {
        self.raw.epoch_tick = tick;
        self
    }

    /// Sets the number of objects that must be in a batch
    /// before reclamation is attempted.
    ///
    /// Retired objects are added to thread-local batches
    /// before the retirement process is actually started. After
    /// `batch_size` is hit, objects are moved to separate retirement
    /// lists, where reference counting kicks in and batches are
    /// eventually reclaimed.
    ///
    /// A larger batch size means that deallocation is done
    /// less frequently, but reclamation also becomes more
    /// expensive due to longer retirement lists needing
    /// to be traversed and freed.
    ///
    /// Note that batch sizes should generally be larger
    /// than the number of threads accessing objects. The default batch
    /// size is `120` nodes.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.raw.batch_size = size;
        self
    }

    /// Mark the current thread as active, returning a guard
    /// that allows protecting loads of atomic pointers. The thread
    /// will be marked as inactive when the guard is dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// # use seize::AtomicPtr;
    /// # use std::sync::atomic::Ordering;
    /// # let collector = seize::Collector::new();
    /// # let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    /// let guard = collector.enter();
    /// let value = guard.protect(&ptr, Ordering::Acquire);
    /// println!("{}", **value);
    /// drop(guard);
    /// // accessing `value` after this is **unsound**
    /// # unsafe { guard.retire(value, seize::reclaim::boxed::<usize>) };
    /// ```
    ///
    /// `enter` is reentrant, so it is legal to create multiple guards
    /// on the same thread. The thread will stay marked as active until
    /// the last guard is dropped:
    ///
    /// ```
    /// # use seize::AtomicPtr;
    /// # use std::sync::atomic::Ordering;
    /// # let collector = seize::Collector::new();
    /// let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    ///
    /// let guard1 = collector.enter();
    /// let guard2 = collector.enter();
    ///
    /// let value = guard2.protect(&ptr, Ordering::Acquire);
    /// drop(guard1);
    /// // the first guard is dropped, but `value` is still safe to access
    /// // while the second guard exists
    /// unsafe { assert_eq!(**value, 1) }
    /// # unsafe { guard2.retire(value, seize::reclaim::boxed::<usize>) };
    /// drop(guard2) // _now_, the thread is marked as inactive
    /// ```
    pub fn enter(&self) -> Guard<'_> {
        self.raw.enter();

        Guard {
            collector: self,
            retire_batch: UnsafeCell::new(false),
            _a: PhantomData,
        }
    }

    /// Link an object to the collector.
    ///
    /// Collectors require reserving some extra memory for each object
    /// that is allocated, so atomic pointers must take the form of
    /// `AtomicPtr<Linked<T>>`, as opposed to `AtomicPtr<T>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use seize::{Collector, Linked};
    /// # use std::mem::ManuallyDrop;
    /// # use std::sync::atomic::{AtomicPtr, Ordering};
    ///
    /// pub struct Stack<T> {
    ///     head: AtomicPtr<Linked<Node<T>>>,
    ///     collector: Collector,
    /// }
    ///
    /// struct Node<T> {
    ///     next: *mut Linked<Node<T>>,
    ///     value: T,
    /// }
    ///
    /// impl<T> Stack<T> {
    ///     pub fn push(&self, value: T) {
    ///         let node = self.collector.link(Node {
    ///             next: std::ptr::null_mut(),
    ///             value,
    ///         });
    ///
    ///         // ...
    ///     }
    /// }
    /// ```
    pub fn link<T>(&self, value: T) -> Linked<T> {
        Linked {
            value,
            node: UnsafeCell::new(self.raw.node()),
        }
    }

    /// Link an object to the collector and allocate it.
    ///
    /// This is equivalent to:
    ///
    /// ```
    /// # let collector = seize::Collector::new();
    /// # let value = 0;
    /// Box::into_raw(Box::new(collector.link(value)))
    /// ```
    pub fn link_boxed<T>(&self, value: T) -> *mut Linked<T> {
        Box::into_raw(Box::new(self.link(value)))
    }

    /// Retire an object, running the reclaimer when it is safe to do so.
    ///
    /// The object will only be reclaimed after all currently active threads
    /// drop their guards, which ensures that the object is no longer being referenced.
    ///
    /// # Examples
    ///
    /// The reclaimer can either be a type from the [`reclaim`](crate::reclaim) module,
    /// or a custom reclaimer function. For example, objects allocated with [`link_boxed`](Self::link_boxed)
    /// can use [`reclaim::Boxed`](crate::reclaim::Boxed):
    ///
    /// ```
    /// use seize::reclaim;
    /// # struct Stack<T> { collector: seize::Collector, _t: T }
    /// # struct Node<T>(T);
    ///
    /// impl<T> Stack<T> {
    ///     fn pop(&self) -> Option<T> {
    ///         # let t = unsafe { std::mem::transmute(()) }; // ¯\_(ツ)_/¯
    ///         # let head = self.collector.link_boxed(Node(t));
    ///         // ...
    ///         unsafe {
    ///             self.collector.retire(head, reclaim::boxed::<Node<T>>); // <===
    ///         }
    ///         // ...
    ///     }
    /// }
    /// ```
    ///
    /// If extra work needs to be done on drop, a custom reclaimer function can be used instead.
    /// Custom reclaimers receive the type-erased [`Link`] struct as a parameter, which must
    /// be casted back to the correct type to retreive the original object:
    ///
    /// ```
    /// # let collector = seize::Collector::new();
    /// # struct Node<T>(T);
    /// # fn x<T>(head: Node<T>) {
    /// collector.retire(head, |link: Link| unsafe {
    ///     // safety: the value passed to retire was of type `*mut Linked<Node<T>>`
    ///     let ptr: *mut Linked<Node<T>> = link.cast::<Node<T>>();
    ///
    ///     // safety: the value was allocated with `link_boxed`
    ///     let head = Box::from_raw(ptr);
    ///     # let head = 1;
    ///     println!("dropping {}", head);
    ///
    ///     drop(head);
    /// });
    /// # }
    /// ```
    ///
    /// # Safety
    ///
    /// Retiring an object is only sound if:
    ///
    /// - The object is no longer accessible to any threads. This generally means a
    ///   new value must have been stored to the relevant atomic pointer.
    /// - The object has not already been retired. Retiring the same object multiple
    ///   times is unsound.
    /// - The object is not accessed again after the call to `retire`. It's possible
    ///   that the object is reclaimed immediately, so any references are invalidated.
    pub unsafe fn retire<T, R>(&self, ptr: *mut Linked<T>, reclaimer: R)
    where
        R: Reclaim<T>,
    {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        unsafe {
            let (should_retire, batch) = self.raw.add(ptr, reclaimer.reclaimer());
            if should_retire {
                self.raw.retire(batch);
            }
        }
    }

    /// Returns true if both references point to the same collector.
    ///
    /// ```
    /// use seize::Collector;
    ///
    /// let a = Collector::new();
    /// let b = Collector::new();
    ///
    /// assert!(Collector::ptr_eq(&a, &a));
    /// assert!(!Collector::ptr_eq(&a, &b));
    /// ```
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
            .epoch_tick(self.raw.epoch_tick)
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

        if self.raw.epoch_tick.is_some() {
            strukt.field("epoch", &self.raw.epoch.load(Ordering::Acquire));
        }

        strukt
            .field("batch_size", &self.raw.batch_size)
            .field("epoch_frequency", &self.raw.epoch_tick)
            .finish()
    }
}

/// A guard that keeps the current thread marked as active.
///
/// See [`Collector::enter`] for details.
pub struct Guard<'a> {
    collector: *const Collector,
    retire_batch: UnsafeCell<bool>,
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
            retire_batch: UnsafeCell::new(false),
            _a: PhantomData,
        }
    }

    /// Protects the load of an atomic pointer.
    ///
    /// Any valid pointer loaded through `protect` is guaranteed to stay valid
    /// until this guard is dropped, or the object is retired.
    ///
    /// Note that the lifetime of a guarded pointer is logically tied to that of
    /// the guard – when the guard is dropped the pointer is invalidated – but a
    /// raw pointer is returned for convenience. Datastructures that return shared
    /// references to objects should ensure that the lifetime of the reference is tied
    /// to the lifetime of a guard.
    ///
    /// ```
    /// # use seize::AtomicPtr;
    /// # use std::sync::atomic::Ordering;
    /// # let collector = seize::Collector::new();
    /// # let ptr = AtomicPtr::new(collector.link_boxed(1_usize));
    /// let guard = collector.enter();
    /// let value = guard.protect(&ptr, Ordering::Acquire);
    /// println!("{}", **value);
    /// drop(guard);
    /// // accessing `value` after this is **unsound**
    /// # unsafe { guard.retire(value, seize::reclaim::boxed::<usize>) };
    /// ```
    #[inline]
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut Linked<T> {
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
    pub unsafe fn retire<T, R>(&self, ptr: *mut Linked<T>, reclaimer: R)
    where
        R: Reclaim<T>,
    {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        if self.collector.is_null() {
            // unprotected guard
            return unsafe { (reclaimer.reclaimer())(Link { node: ptr as _ }) };
        }

        unsafe {
            let (should_retire, _) = (*self.collector).raw.add(ptr, reclaimer.reclaimer());
            *self.retire_batch.get() |= should_retire;
        }
    }

    /// Get a reference to the collector this guard we created from.
    ///
    /// This method is useful if you need to ensure that all guards
    /// used with a data structure come from the same collector.
    ///
    /// If this is an [`unprotected`](Guard::unprotected) guard
    /// this method will return `None`.
    pub fn collector(&self) -> Option<&Collector> {
        unsafe { self.collector.as_ref() }
    }

    /// Flush any previous reservations.
    ///
    /// This method notifies other threads that the current thread
    /// is no longer holding on to any protected pointers. If
    /// the current thread holds the last reference to any
    /// retired pointers, they will be reclaimed.
    ///
    /// The only difference between flushing and dropping a guard is
    /// that the current thread stays marked as active, meaning new pointers
    /// can be protected after a call to `flush`.
    ///
    /// # Safety
    ///
    /// This method is not marked as `unsafe`, but will invalidate any
    /// previously protected pointers, similar to dropping a guard. It
    /// is intended to be used safely by users of concurrent data structures,
    /// as references will be tied to the guard and this method takes `&mut self`.
    ///
    /// If called on an [`unprotected`](Guard::unprotected) guard this method is a no-op.
    pub fn flush(&mut self) {
        if self.collector.is_null() {
            return;
        }

        unsafe { (*self.collector).raw.flush() }
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if self.collector.is_null() {
            return;
        }

        unsafe {
            (*self.collector).raw.leave();

            if *self.retire_batch.get() {
                (*self.collector).raw.retire_batch();
            }
        }
    }
}

impl fmt::Debug for Guard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Guard").finish()
    }
}

/// A type-erased [`Linked<T>`].
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
#[repr(C)]
pub struct Linked<T> {
    pub(crate) node: UnsafeCell<raw::Node>, // Safety Invariant: this field must come first
    value: T,
}

impl<T> Linked<T> {
    /// Unwraps the inner value.
    pub fn into_inner(linked: Linked<T>) -> T {
        linked.value
    }
}

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

/// A linked atomic pointer.
///
/// This is simply a type alias for `std::AtomicPtr<Linked<T>>`.
pub type AtomicPtr<T> = std::sync::atomic::AtomicPtr<Linked<T>>;
