use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, Ordering};

use crate::raw::Reservation;
use crate::tls::Thread;
use crate::{AsLink, Collector, Link};

/// A guard that enables protected loads of concurrent objects.
///
/// This trait provides common functionality implemented by [`LocalGuard`],
/// [`OwnedGuard`], and [`UnprotectedGuard`].
///
/// See [the guide](crate::guide#starting-operations) for an introduction to
/// using guards.
pub trait Guard {
    /// Refreshes the guard.
    ///
    /// Calling this method is similar to dropping and immediately
    /// creating a new guard. The current thread remains active, but any
    /// pointers that were previously protected may be reclaimed.
    ///
    /// # Safety
    ///
    /// This method is not marked as `unsafe`, but will affect
    /// the validity of pointers returned by [`protect`](Guard::protect),
    /// similar to dropping a guard. It is intended to be used safely
    /// by users of concurrent data structures, as references will
    /// be tied to the guard and this method takes `&mut self`.
    fn refresh(&mut self);

    /// Flush any retired values in the local batch.
    ///
    /// This method flushes any values from the current thread's local
    /// batch, starting the reclamation process. Note that no memory
    /// can be reclaimed while this guard is active, but calling `flush`
    /// may allow memory to be reclaimed more quickly after the guard is
    /// dropped.
    ///
    /// See [`Collector::batch_size`] for details about batching.
    fn flush(&self);

    /// Protects the load of an atomic pointer.
    ///
    /// Any valid pointer loaded through a guard using the `protect` method is
    /// guaranteed to stay valid until the guard is dropped, or the object
    /// is retired by the current thread. Importantly, if another thread
    /// retires this object, it will not be reclaimed for the lifetime of
    /// this guard.
    ///
    /// Note that the lifetime of a guarded pointer is logically tied to that of
    /// the guard -- when the guard is dropped the pointer is invalidated --
    /// but a raw pointer is returned for convenience. Data structures that
    /// return shared references to values should ensure that the lifetime
    /// of the reference is tied to the lifetime of a guard.
    fn protect<T: AsLink>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T;

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    ///
    /// This method delays reclamation until the guard is dropped as opposed to
    /// [`Collector::retire`], which may reclaim objects immediately.
    ///
    ///
    /// # Safety
    ///
    /// The retired object must no longer be accessible to any thread that
    /// enters after it is removed.
    ///
    /// Retiring the same pointer twice can cause **undefined behavior**, even
    /// if the reclaimer doesn't free memory.
    ///
    /// Additionally, the pointer must be valid to access as a [`Link`], per the
    /// [`AsLink`] trait, and the reclaimer passed to `retire` must
    /// correctly free values of type `T`.
    unsafe fn defer_retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link));

    /// Returns a numeric identifier for the current thread.
    ///
    /// Guards rely on thread-local state, including thread IDs. If you already
    /// have a guard you can use this method to get a cheap identifier for the
    /// current thread, avoiding TLS overhead. Note that thread IDs may be
    /// reused, so the value returned is only unique for the lifetime of
    /// this thread.
    fn thread_id(&self) -> usize;

    /// Returns `true` if this guard belongs to the given collector.
    ///
    /// This can be used to verify that user-provided guards are valid
    /// for the expected collector.
    fn belongs_to(&self, collector: &Collector) -> bool;
}

/// A guard that keeps the current thread marked as active.
///
/// Local guards are created by calling [`Collector::enter`]. Unlike
/// [`OwnedGuard`], a local guard is tied to the current thread and does not
/// implement `Send`. This makes local guards relatively cheap to create and
/// destroy.
///
/// Most of the functionality provided by this type is through the [`Guard`]
/// trait.
pub struct LocalGuard<'a> {
    collector: &'a Collector,
    // The current thread.
    thread: Thread,
    // The reservation of the current thread.
    reservation: *const Reservation,
    // `LocalGuard` not be `Send or Sync` as we are tied to the state of the
    // current thread in the collector.
    _unsend: PhantomData<*mut ()>,
}

impl LocalGuard<'_> {
    #[inline]
    pub(crate) fn enter(collector: &Collector) -> LocalGuard<'_> {
        let thread = Thread::current();
        // Safety: `thread` is the current thread.
        let reservation = unsafe { collector.raw.reservation(thread) };

        // Calls to `enter` may be reentrant, so we need to keep track of the number
        // of active guards for the current thread.
        let guards = reservation.guards.get();
        reservation.guards.set(guards + 1);

        if guards == 0 {
            // Safety: Only called on the current thread, which is currently inactive.
            unsafe { collector.raw.enter(reservation) };
        }

        LocalGuard {
            thread,
            reservation,
            collector,
            _unsend: PhantomData,
        }
    }
}

impl Guard for LocalGuard<'_> {
    /// Protects the load of an atomic pointer.
    #[inline]
    fn protect<T: AsLink>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        // Safety: `self.reservation` is owned by the current thread.
        unsafe {
            self.collector
                .raw
                .protect_local(ptr, ordering, &*self.reservation)
        }
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    #[inline]
    unsafe fn defer_retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link)) {
        // Safety:
        // - `self.thread` is the current thread.
        // - The validity of the pointer is guaranteed by the caller.
        unsafe { self.collector.raw.add(ptr, reclaim, self.thread) }
    }

    /// Refreshes the guard.
    #[inline]
    fn refresh(&mut self) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        let guards = reservation.guards.get();

        if guards == 1 {
            // Safety: We have a unique reference to the last active guard.
            unsafe { self.collector.raw.refresh(reservation) }
        }
    }

    /// Flush any retired values in the local batch.
    #[inline]
    fn flush(&self) {
        // Note that this does not actually retire any values, it just attempts
        // to add the batch to any active reservations lists, including ours.
        //
        // Safety: `self.thread` is the current thread.
        unsafe { self.collector.raw.try_retire_batch(self.thread) }
    }

    /// Returns a numeric identifier for the current thread.
    #[inline]
    fn thread_id(&self) -> usize {
        self.thread.id
    }

    /// Returns `true` if this guard belongs to the given collector.
    #[inline]
    fn belongs_to(&self, collector: &Collector) -> bool {
        Collector::id_eq(self.collector, collector)
    }
}

impl Drop for LocalGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };

        // Decrement the active guard count.
        let guards = reservation.guards.get();
        reservation.guards.set(guards - 1);

        if guards == 1 {
            // Safety: We have a unique reference to the last active guard.
            unsafe { self.collector.raw.leave(reservation) };
        }
    }
}

impl fmt::Debug for LocalGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LocalGuard").finish()
    }
}

/// A guard that protects objects for it's lifetime, independent of the current
/// thread.
///
/// Unlike [`LocalGuard`], an owned guard is independent of the current thread,
/// allowing them to implement `Send`. This is useful for holding guards across
/// `.await` points in work-stealing schedulers, where execution may be resumed
/// on a different thread than started on. However, owned guards are more
/// expensive to create and destroy, so should be avoided if cross-thread usage
/// is not required.
///
/// Most of the functionality provided by this type is through the [`Guard`]
/// trait.
pub struct OwnedGuard<'a> {
    collector: &'a Collector,
    // An owned thread, unique to this guard.
    thread: Thread,
    // The reservation of this guard.
    reservation: *const Reservation,
}

// Safety: `OwnedGuard` owns its thread slot, so is not tied to any
// thread-locals
unsafe impl Send for OwnedGuard<'_> {}
unsafe impl Sync for OwnedGuard<'_> {}

impl OwnedGuard<'_> {
    #[inline]
    pub(crate) fn enter(collector: &Collector) -> OwnedGuard<'_> {
        // Create a thread slot that will last for the lifetime of this guard.
        let thread = Thread::create();

        // Safety: We have ownership of `thread`.
        let reservation = unsafe { collector.raw.reservation(thread) };

        // Safety: We have ownership of `reservation`.
        unsafe { collector.raw.enter(reservation) };

        OwnedGuard {
            collector,
            thread,
            reservation,
        }
    }
}

impl Guard for OwnedGuard<'_> {
    /// Protects the load of an atomic pointer.
    #[inline]
    fn protect<T: AsLink>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        self.collector.raw.protect(ptr, ordering, reservation)
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    #[inline]
    unsafe fn defer_retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link)) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        let _lock = reservation.lock.lock().unwrap();

        // Safety:
        // - We hold the lock and so have unique access to the batch.
        // - The validity of the pointer is guaranteed by the caller.
        unsafe { self.collector.raw.add(ptr, reclaim, self.thread) }
    }

    /// Refreshes the guard.
    #[inline]
    fn refresh(&mut self) {
        // Safety: We have `&mut self` and ownership of the thread.
        unsafe {
            self.collector
                .raw
                .refresh(self.collector.raw.reservation(self.thread))
        }
    }

    /// Flush any retired values in the local batch.
    #[inline]
    fn flush(&self) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        let _lock = reservation.lock.lock().unwrap();
        // Note that this does not actually retire any values, it just attempts
        // to add the batch to any active reservations lists, including ours.
        //
        // Safety: We hold the lock and so have unique access to the batch.
        unsafe { self.collector.raw.try_retire_batch(self.thread) }
    }

    /// Returns a numeric identifier for the current thread.
    #[inline]
    fn thread_id(&self) -> usize {
        // We can't return the ID of our thread slot because `OwnedGuard`
        // is `Send` so the ID is not uniquely tied to the current thread.
        // We also can't return the OS thread ID because it might conflict
        // with our thread IDs, so we have to get/create the current thread.
        Thread::current().id
    }

    /// Returns `true` if this guard belongs to the given collector.
    #[inline]
    fn belongs_to(&self, collector: &Collector) -> bool {
        Collector::id_eq(self.collector, collector)
    }
}

impl Drop for OwnedGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };

        // Safety: `self.thread` is an owned thread.
        unsafe { self.collector.raw.leave(reservation) };

        // Safety: We are in `drop` and never share `self.thread`.
        unsafe { self.thread.free() };
    }
}

/// Returns a dummy guard object.
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
#[inline]
pub unsafe fn unprotected() -> UnprotectedGuard {
    UnprotectedGuard
}

/// A dummy guard object.
///
/// See [`unprotected`] for details.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct UnprotectedGuard;

impl Guard for UnprotectedGuard {
    /// Loads the pointer directly, using the given ordering.
    #[inline]
    fn protect<T: AsLink>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        ptr.load(ordering)
    }

    /// Reclaims the pointer immediately.
    #[inline]
    unsafe fn defer_retire<T: AsLink>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link)) {
        unsafe { reclaim(ptr.cast::<Link>()) }
    }

    /// This method is a no-op.
    #[inline]
    fn refresh(&mut self) {}

    /// This method is a no-op.
    #[inline]
    fn flush(&self) {}

    /// Returns a numeric identifier for the current thread.
    #[inline]
    fn thread_id(&self) -> usize {
        Thread::current().id
    }

    /// Unprotected guards aren't tied to a specific collector, so this always
    /// returns `true`.
    #[inline]
    fn belongs_to(&self, _collector: &Collector) -> bool {
        true
    }
}
