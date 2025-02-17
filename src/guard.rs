use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, Ordering};

use crate::raw::{self, Reservation, Thread};
use crate::Collector;

/// A guard that enables protected loads of concurrent objects.
///
/// This trait provides common functionality implemented by [`LocalGuard`] and
/// [`OwnedGuard`]. See [the guide](crate::guide#starting-operations) for an
/// introduction to using guards.
pub trait Guard {
    /// Refreshes the guard.
    ///
    /// Calling this method is similar to dropping and immediately creating a
    /// new guard. The current thread remains active, but any pointers that
    /// were previously protected may be reclaimed.
    ///
    /// # Safety
    ///
    /// This method is not marked as `unsafe`, but will affect the validity of
    /// pointers loaded using [`Guard::protect`], similar to dropping a guard.
    /// It is intended to be used safely by users of concurrent data structures,
    /// as references will be tied to the guard and this method takes `&mut self`.
    fn refresh(&mut self);

    /// Flush any retired values in the local batch.
    ///
    /// This method flushes any values from the current thread's local batch,
    /// starting the reclamation process. Note that no memory can be
    /// reclaimed while this guard is active, but calling `flush` may allow
    /// memory to be reclaimed more quickly after the guard is dropped.
    ///
    /// Note that the batch must contain at least as many objects as the number
    /// of currently active threads for a flush to be performed. See
    /// [`Collector::batch_size`] for details about batch sizes.
    fn flush(&self);

    /// Returns the collector this guard was created from.
    fn collector(&self) -> &Collector;

    /// Returns a numeric identifier for the current thread.
    ///
    /// Guards rely on thread-local state, including thread IDs. This method is
    /// a cheap way to get an identifier for the current thread without TLS
    /// overhead. Note that thread IDs may be reused, so the value returned
    /// is only unique for the lifetime of this thread.
    ///
    fn thread_id(&self) -> usize;
    /// Protects the load of an atomic pointer.
    ///
    /// Any valid pointer loaded through a guard using the `protect` method is
    /// guaranteed to stay valid until the guard is dropped, or the object
    /// is retired by the current thread. Importantly, if another thread
    /// retires this object, it will not be reclaimed for the lifetime of
    /// this guard.
    ///
    /// Note that the lifetime of a guarded pointer is logically tied to that of
    /// the guard — when the guard is dropped the pointer is invalidated. Data
    /// structures that return shared references to values should ensure that the
    /// lifetime of the reference is tied to the lifetime of a guard.
    fn protect<T>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        ptr.load(raw::Collector::protect(ordering))
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    ///
    /// This method delays reclamation until the guard is dropped, as opposed to
    /// [`Collector::retire`], which may reclaim objects immediately.
    ///
    ///
    /// # Safety
    ///
    /// The retired pointer must no longer be accessible to any thread that
    /// enters after it is removed. Additionally, the pointer must be valid
    /// to pass to the provided reclaimer, once it is safe to reclaim.
    unsafe fn defer_retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector));
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
    /// The collector that this guard is associated with.
    collector: &'a Collector,

    // The current thread.
    thread: Thread,

    // The reservation for the current thread.
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

        // Calls to `enter` may be reentrant, so we need to keep track of the number of
        // active guards for the current thread.
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
        // Note that this does not actually retire any values, it just attempts to add
        // the batch to any active reservations lists, including ours.
        //
        // Safety: `self.thread` is the current thread.
        unsafe { self.collector.raw.try_retire_batch(self.thread) }
    }

    /// Returns the collector this guard was created from.
    #[inline]
    fn collector(&self) -> &Collector {
        self.collector
    }

    /// Returns a numeric identifier for the current thread.
    #[inline]
    fn thread_id(&self) -> usize {
        self.thread.id
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    #[inline]
    unsafe fn defer_retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        // Safety:
        // - `self.thread` is the current thread.
        // - The validity of the pointer is guaranteed by the caller.
        unsafe { self.collector.raw.add(ptr, reclaim, self.thread) }
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
/// allowing it to implement `Send` and `Sync`. This is useful for holding
/// guards across `.await` points in work-stealing schedulers, where execution
/// may be resumed on a different thread than started on. However, owned guards
/// are more expensive to create and destroy, so should be avoided if
/// cross-thread usage is not required.
///
/// Most of the functionality provided by this type is through the [`Guard`]
/// trait.
pub struct OwnedGuard<'a> {
    /// The collector that this guard is associated with.
    collector: &'a Collector,

    // An owned thread, unique to this guard.
    thread: Thread,

    // The reservation for this guard.
    reservation: *const Reservation,
}

// Safety: All shared methods on `OwnedGuard` that access shared memory are
// synchronized with locks.
unsafe impl Sync for OwnedGuard<'_> {}

// Safety: `OwnedGuard` owns its thread slot and is not tied to any
// thread-locals.
unsafe impl Send for OwnedGuard<'_> {}

impl OwnedGuard<'_> {
    #[inline]
    pub(crate) fn enter(collector: &Collector) -> OwnedGuard<'_> {
        // Create a thread slot that will last for the lifetime of this guard.
        let thread = Thread::create();

        // Safety: We have ownership of `thread` and have not shared it.
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
    /// Refreshes the guard.
    #[inline]
    fn refresh(&mut self) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        unsafe { self.collector.raw.refresh(reservation) }
    }

    /// Flush any retired values in the local batch.
    #[inline]
    fn flush(&self) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        let _lock = reservation.lock.lock().unwrap();
        // Note that this does not actually retire any values, it just attempts to add
        // the batch to any active reservations lists, including ours.
        //
        // Safety: We hold the lock and so have unique access to the batch.
        unsafe { self.collector.raw.try_retire_batch(self.thread) }
    }

    /// Returns the collector this guard was created from.
    #[inline]
    fn collector(&self) -> &Collector {
        self.collector
    }

    /// Returns a numeric identifier for the current thread.
    #[inline]
    fn thread_id(&self) -> usize {
        // We can't return the ID of our thread slot because `OwnedGuard` is `Send` so
        // the ID is not uniquely tied to the current thread. We also can't
        // return the OS thread ID because it might conflict with our thread
        // IDs, so we have to get/create the current thread.
        Thread::current().id
    }

    /// Retires a value, running `reclaim` when no threads hold a reference to
    /// it.
    #[inline]
    unsafe fn defer_retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        // Safety: `self.reservation` is owned by the current thread.
        let reservation = unsafe { &*self.reservation };
        let _lock = reservation.lock.lock().unwrap();

        // Safety:
        // - We hold the lock and so have unique access to the batch.
        // - The validity of the pointer is guaranteed by the caller.
        unsafe { self.collector.raw.add(ptr, reclaim, self.thread) }
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
        unsafe { Thread::free(self.thread.id) };
    }
}
