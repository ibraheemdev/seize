use crate::tls::{Thread, ThreadLocal};
use crate::utils::CachePadded;
use crate::{AsLink, Deferred, Link};

use std::cell::{Cell, UnsafeCell};
use std::num::NonZeroU64;
use std::ptr;
use std::sync::atomic::{self, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Fast, lock-free, and robust concurrent memory reclamation.
///
/// The core algorithm is described [in this paper](https://arxiv.org/pdf/2108.02763.pdf).
pub struct Collector {
    /// The global epoch value.
    ///
    /// The global epoch is used to detect stalled threads. The current epoch
    /// is recorded on each allocated object as well as every time a thread
    /// attempts to access a object. This allows retirers to skip threads that
    /// are lagging behind and could not have accessed a given object.
    pub(crate) epoch: AtomicU64,

    /// Per-thread batches of retired nodes.
    ///
    /// Retired values are added to thread-local batches before starting
    /// the reclamation process, to amortize the cost of retirement.
    batches: ThreadLocal<CachePadded<UnsafeCell<LocalBatch>>>,

    /// Per-thread reservations lists.
    ///
    /// A reservation list is a list of batches that have been retired
    /// while the current thread was active. The thread must decrement
    /// the reference count and potentially free the batch of any
    /// reservations before exiting.
    reservations: ThreadLocal<CachePadded<Reservation>>,

    /// The number of object allocations before advancing the global epoch
    /// on a given thread.
    ///
    /// If this is `None`, epoch tracking is disabled.
    pub(crate) epoch_frequency: Option<NonZeroU64>,

    /// The minimum number of nodes required in a batch before freeing.
    pub(crate) batch_size: usize,
}

impl Collector {
    /// Create a collector with the provided configuration.
    pub fn new(threads: usize, epoch_frequency: NonZeroU64, batch_size: usize) -> Self {
        Self {
            // Note the global epoch must start at 1 because thread-local epochs
            // start at 0. All threads must be forced to synchronize when protecting
            // objects created in the first epoch.
            epoch: AtomicU64::new(1),
            reservations: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            epoch_frequency: Some(epoch_frequency),
            batch_size,
        }
    }

    /// Create a new node.
    ///
    /// # Safety
    ///
    /// This method is not safe to call concurrently on the same thread.
    #[inline]
    pub fn node(&self, reservation: &Reservation) -> Node {
        // Record the current epoch value.
        //
        // Note that it's fine if we see an older epoch, in which case more threads
        // may be counted as active.
        let birth_epoch = match self.epoch_frequency {
            Some(ref frequency) => {
                // Safety: Node counts are only accessed by the current thread.
                let count = reservation.node_count.get();
                reservation.node_count.set(count + 1);

                // Advance the global epoch if we reached the epoch frequency.
                //
                // Relaxed: Synchronization only occurs when an epoch is recorded by a
                // given thread to protect a pointer, not when incrementing the epoch.
                if count >= frequency.get() {
                    reservation.node_count.set(0);
                    self.epoch.fetch_add(1, Ordering::Relaxed) + 1
                } else {
                    self.epoch.load(Ordering::Relaxed)
                }
            }

            // We aren't tracking epochs.
            None => 0,
        };

        Node { birth_epoch }
    }

    /// Return the reservation for the current thread.
    ///
    /// # Safety
    ///
    /// The reservation must not be accessed concurrently across multiple
    /// threads without correct synchronization.
    #[inline]
    pub unsafe fn reservation(&self, thread: Thread) -> &Reservation {
        self.reservations.load(thread)
    }

    /// Mark the current thread as active.
    ///
    /// `enter` and `leave` calls maintain a reference count to allow
    /// reentrancy. If the current thread is already marked as active, this
    /// method simply increases the reference count.
    ///
    /// # Safety
    ///
    /// This method is not safe to call concurrently on the same thread. This
    /// method must only be called if the current thread is inactive.
    #[inline]
    pub unsafe fn enter(&self, reservation: &Reservation) {
        // Mark the current thread as active.
        //
        // SeqCst: Establish a total order between this store and the fence in `retire`.
        // - If our store comes first, the thread retiring will see that we are active.
        // - If the fence comes first, we will see the new values of any objects being
        //   retired by that thread
        //
        // Note that all pointer loads must also be SeqCst and thus participate in this
        // total order.
        reservation.head.store(ptr::null_mut(), Ordering::SeqCst);
    }

    /// Load an atomic pointer.
    ///
    /// # Safety
    ///
    /// This method must only be called with the reservation of the current thread.
    #[inline]
    pub unsafe fn protect_local<T>(
        &self,
        ptr: &AtomicPtr<T>,
        _ordering: Ordering,
        reservation: &Reservation,
    ) -> *mut T {
        if self.epoch_frequency.is_none() {
            // Epoch tracking is disabled.
            //
            // Note that any protected loads still need to be SeqCst to participate in the
            // total order. See `enter` for details.
            return ptr.load(Ordering::SeqCst);
        }

        // Load the last epoch we recorded on this thread.
        //
        // Relaxed: The reservation is only modified by the current thread.
        let mut prev_epoch = reservation.epoch.load(Ordering::Relaxed);

        loop {
            // SeqCst:
            // - Ensure that this load participates in the total order. See `enter` for
            //   details.
            // - Acquire the birth epoch of the node. We need to record at least the birth
            //   epoch below to let other threads know we are accessing this pointer. Note
            //   that this requires the pointer to have been stored with Release ordering,
            //   which is technically undocumented. However, any Relaxed stores would be
            //   unsound to access anyways.
            let ptr = ptr.load(Ordering::SeqCst);

            // Relaxed: We acquired at least the pointer's birth epoch above.
            let current_epoch = self.epoch.load(Ordering::Relaxed);

            // We are marked as active in the birth epoch of the pointer we are accessing.
            // Any threads performing retirement will see that we have access to the pointer
            // and add to our reservation list.
            if prev_epoch == current_epoch {
                return ptr;
            }

            // Our epoch is out of date, record the new one and try again.
            //
            // SeqCst: Establish a total order between this store and the fence in `retire`.
            // - If our store comes first, the thread retiring will see that we are active
            //   in the current epoch.
            // - If the fence comes first, we will see the new values of any objects being
            //   retired by that thread.
            reservation.epoch.store(current_epoch, Ordering::SeqCst);
            prev_epoch = current_epoch;
        }
    }

    /// Load an atomic pointer.
    ///
    /// This method is safe to call concurrently from multiple threads with the
    /// same `thread` object.
    #[inline]
    pub fn protect<T>(
        &self,
        ptr: &AtomicPtr<T>,
        _ordering: Ordering,
        reservation: &Reservation,
    ) -> *mut T {
        if self.epoch_frequency.is_none() {
            // Epoch tracking is disabled.
            //
            // Note that any protected loads still need to be SeqCst to participate in the
            // total order. See `enter` for details.
            return ptr.load(Ordering::SeqCst);
        }

        // Load the last epoch we recorded for this reservation.
        //
        // SeqCst: This epoch may be modified concurrently by other threads. If
        // a different thread recorded an epoch, we must force this thread to also
        // participate in the total order and load the new values of any objects
        // that may have been retired.
        let mut prev_epoch = reservation.epoch.load(Ordering::SeqCst);

        loop {
            // SeqCst:
            // - Ensure that this load participates in the total order. See `enter` for
            //   details.
            // - Acquire the birth epoch of the node. We need to record at least the birth
            //   epoch below to let other threads know we are accessing this pointer.
            let ptr = ptr.load(Ordering::SeqCst);

            // Relaxed: We acquired at least the pointer's birth epoch above.
            let current_epoch = self.epoch.load(Ordering::Relaxed);

            // We are marked as active in the birth epoch of the pointer we are accessing.
            // Any threads performing retirement will see that we have access to the pointer
            // and add to our reservation list.
            if prev_epoch == current_epoch {
                return ptr;
            }

            // Our epoch is out of date, record the new one and try again.
            //
            // SeqCst: Establish a total order between this store and the fence in `retire`.
            // - If our store comes first, the thread retiring will see that we are active
            //   in the current epoch.
            // - If the fence comes first, we will see the new values of any objects being
            //   retired by that thread.
            //
            // Note that this may be called concurrently, so the `fetch_max` ensures we
            // never overwrite a newer epoch. If a different thread beats us and writes
            // a newer epoch, the SeqCst load guarantees that we still participate in the
            // total order.
            prev_epoch = reservation
                .epoch
                .fetch_max(current_epoch, Ordering::SeqCst)
                .max(current_epoch);
        }
    }

    /// Mark the current thread as inactive.
    ///
    /// # Safety
    ///
    /// Any previously protected pointers may be invalidated after calling
    /// `leave`. Additionally, method is not safe to call concurrently with
    /// the same reservation.
    #[inline]
    pub unsafe fn leave(&self, reservation: &Reservation) {
        // Release: Exit the critical section, ensuring that any pointer accesses
        // happen-before we are marked as inactive.
        let head = reservation.head.swap(Entry::INACTIVE, Ordering::Release);

        if head != Entry::INACTIVE {
            // Acquire any new entries in the reservation list, as well as the new values
            // of any objects that were retired while we were active.
            atomic::fence(Ordering::Acquire);

            // Decrement the reference counts of any batches that were retired.
            unsafe { Collector::traverse(head) }
        }
    }

    /// Decrement any reference counts, keeping the thread marked as active.
    ///
    /// # Safety
    ///
    /// Any previously protected pointers may be invalidated after calling
    /// `leave`. Additionally, method is not safe to call concurrently with
    /// the same reservation.
    #[inline]
    pub unsafe fn refresh(&self, reservation: &Reservation) {
        // This stores acts as a combined call to `leave` and `enter`.
        //
        // SeqCst: Establish the same ordering as `enter` and `leave`.
        let head = reservation.head.swap(ptr::null_mut(), Ordering::SeqCst);

        if head != Entry::INACTIVE {
            // Decrement the reference counts of any batches that were retired.
            unsafe { Collector::traverse(head) }
        }
    }

    /// Add a node to the retirement batch, retiring the batch if `batch_size`
    /// is reached.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer that is no longer accessible to any
    /// inactive threads. Additionally, this method is not safe to call
    /// concurrently with the same `thread`.
    #[inline]
    pub unsafe fn add<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link), thread: Thread)
    where
        T: AsLink,
    {
        let local_batch = self.batches.load(thread).get();

        // Safety: Local batches are only accessed by the current thread.
        let batch = unsafe { (*local_batch).get_or_init(self.batch_size) };

        // If we are in a recursive call during `drop` or `reclaim_all`, reclaim the
        // object immediately.
        if batch == LocalBatch::DROP {
            unsafe { reclaim(ptr.cast::<Link>()) }
            return;
        }

        // `ptr` is guaranteed to be a valid pointer that can be cast to a node
        // (because of `T: AsLink`).
        //
        // Any other thread with a reference to the pointer only has a shared reference
        // to the `UnsafeCell<Node>`, which is allowed to alias. The caller guarantees
        // that the same pointer is not retired twice, so we can safely write to the
        // node through the shared pointer.
        let node = UnsafeCell::raw_get(ptr.cast::<UnsafeCell<Node>>());

        // Safety: The node and batch are both valid for mutable access.
        let len = unsafe {
            // Keep track of the oldest node in the batch.
            let birth_epoch = (*node).birth_epoch;
            (*batch).min_epoch = (*batch).min_epoch.min(birth_epoch);

            // Create an entry for this node.
            (*batch).entries.push(Entry {
                node,
                reclaim,
                batch,
            });

            (*batch).entries.len()
        };

        // Attempt to retire the batch if we have enough entries.
        if len % self.batch_size == 0 {
            // Safety: The caller guarantees that this method is not called concurrently
            // with the same `thread` and we are not holding on to any mutable references.
            unsafe { self.try_retire(local_batch, thread) }
        }
    }

    // Retire a batch of nodes.
    //
    // # Safety
    //
    // The batch must no longer accessible to any inactive threads.
    /// Additionally, this method is not safe to call concurrently with the same
    /// `thread`.
    #[inline]
    pub unsafe fn add_batch(
        &self,
        deferred: &mut Deferred,
        reclaim: unsafe fn(*mut Link),
        thread: Thread,
    ) {
        let local_batch = self.batches.load(thread).get();

        // Safety: Local batches are only accessed by the current thread.
        let batch = unsafe { (*local_batch).get_or_init(self.batch_size) };

        // If we are in a recursive call during `drop` or `reclaim_all`, reclaim the
        // batch immediately.
        if batch == LocalBatch::DROP {
            deferred.for_each(|ptr| unsafe { reclaim(ptr.cast::<Link>()) });
            return;
        }

        let mut should_retire = false;
        let min_epoch = *deferred.min_epoch.get_mut();

        // Safety: The deferred and local batch are both valid for mutable access.
        unsafe {
            // Keep track of the oldest node in the batch.
            (*batch).min_epoch = (*batch).min_epoch.min(min_epoch);

            deferred.for_each(|node| {
                // Create an entry for this node.
                (*batch).entries.push(Entry {
                    node,
                    reclaim,
                    batch,
                });

                // We want to keep retirement amortized consistently, so only retire if we
                // reach a multiple of the batch size
                should_retire = should_retire || (*batch).entries.len() % self.batch_size == 0;
            });
        }

        // Attempt to retire the batch if we have enough entries.
        if should_retire {
            // Safety: The caller guarantees that this method is not called concurrently
            // with the same `thread` and we are not holding on to any mutable references.
            unsafe { self.try_retire(local_batch, thread) }
        }
    }

    /// Attempt to retire objects in the current thread's batch.
    ///
    /// # Safety
    ///
    /// This method is not safe to call concurrently with the same `thread`.
    #[inline]
    pub unsafe fn try_retire_batch(&self, thread: Thread) {
        let local_batch = self.batches.load(thread).get();

        // Safety: caller guarantees this method is not called concurrently with the
        // same `thread`.
        unsafe { self.try_retire(local_batch, thread) }
    }

    /// Attempt to retire objects in this batch.
    ///
    /// Note that if a guard on the current thread is active, the batch will
    /// also be added to the current reservation list for deferred reclamation.
    ///
    /// # Safety
    ///
    /// This method is not safe to call concurrently with the same `thread`.
    /// Additionally, the caller should not be holding on to any mutable
    /// references the the local batch, as they may be invalidated by
    /// recursive calls to `try_retire`.
    #[inline]
    pub unsafe fn try_retire(&self, local_batch: *mut LocalBatch, thread: Thread) {
        // Establish a total order between the retirement of nodes in this batch and
        // stores marking a thread as active, or active in an epoch:
        // - If the store comes first, we will see that the thread is active.
        // - If this fence comes first, the thread will see the new values of any
        //   objects in this batch.
        //
        // This fence also establishes synchronizes with the fence run when a thread is
        // created:
        // - If our fence comes first, they will see the new values of any objects in
        //   this batch.
        // - If their fence comes first, we will see the new thread.
        atomic::fence(Ordering::SeqCst);

        // Safety: Local batches are only accessed by the current thread.
        let batch = unsafe { (*local_batch).batch };

        // There is nothing to retire.
        if batch.is_null() || batch == LocalBatch::DROP {
            return;
        }

        // Safety: The batch is non-null.
        let batch_entries = unsafe { (*batch).entries.as_mut_ptr() };
        let current_reservation = self.reservations.load(thread);
        let mut marked = 0;

        // Record all active threads, including the current thread.
        //
        // We need to do this in a separate step before actually retiring the batch to
        // ensure we have enough entries for reservation lists, as the number of threads
        // can grow dynamically.
        for reservation in self.reservations.iter() {
            // If we don't have enough entries to insert into the reservation lists
            // of all active threads, try again later.
            //
            // Safety: Local batch pointers are valid until relamation.
            let Some(entry) = unsafe { &(*batch).entries }.get(marked) else {
                return;
            };

            // If this thread is inactive, we can skip it. The SeqCst fence above
            // ensurse that the next time it becomes active, it will see the new
            // values of any objects in this batch.
            //
            // Relaxed: See the Acquire fence below.
            if reservation.head.load(Ordering::Relaxed) == Entry::INACTIVE {
                continue;
            }

            // If this thread's epoch is behind the earliest birth epoch in this batch
            // we can skip it, as there is no way it could have accessed any of the objects
            // in this batch.  The SeqCst fence above ensurse that the next time it attempts
            // to access an object in this batch in `protect`, it will see it's new value.
            //
            // We make sure never to skip the current thread even if it's epoch is behind
            // because it may still have access to the pointer. The current thread is only
            // skipped if there is no active guard.
            //
            // Relaxed: We already ensured that we will see the relevant epoch through the
            // SeqCst fence above. If the epoch is behind there is nothing to synchronize
            // with.
            //
            // If epoch tracking is disabled this is always false (0 < 0).
            if !ptr::eq(reservation, current_reservation)
                && reservation.epoch.load(Ordering::Relaxed) < unsafe { (*batch).min_epoch }
            {
                continue;
            }

            // Temporarily store this reservation list in the batch.
            //
            // Safety: All nodes in a batch are valid and this batch has not yet been shared
            // to other threads.
            unsafe { (*entry.node).head = &reservation.head }
            marked += 1;
        }

        // For any inactive threads we skipped above, synchronize with `leave` to ensure
        // any accesses happen-before we retire. We ensured with the SeqCst fence above
        // that the thread will see the new values of any objects in this batch the next
        // time it becomes active.
        atomic::fence(Ordering::Acquire);

        // Add the batch to the reservation lists of any active threads.
        let mut active = 0;
        'retire: for i in 0..marked {
            // Safety: Local batch pointers are valid until reclamation.
            let curr = unsafe { batch_entries.add(i) };

            // Safety: All nodes in the batch are valid and we just initialized `head` for
            // all `marked` nodes in the loop above.
            let head = unsafe { &*(*(*curr).node).head };

            // Relaxed: All writes to the `head` use RMW instructions, so the previous node
            // in the list is synchronized through the release sequence on `head`.
            let mut prev = head.load(Ordering::Relaxed);

            loop {
                // The thread became inactive, skip it.
                //
                // As long as the thread became inactive at some point after the SeqCst fence,
                // it can no longer access any objects in this batch. The next time it becomes
                // active it will load the new object values due to the SeqCst fence above.
                if prev == Entry::INACTIVE {
                    // Acquire: Synchronize with `leave` to ensure any accesses happen-before we
                    // retire.
                    atomic::fence(Ordering::Acquire);
                    continue 'retire;
                }

                // Link this node to the reservation list.
                unsafe { (*(*curr).node).next = prev }

                // Release the node, as well as the new values of any objects in this batch for
                // when this thread calls `leave`.
                match head.compare_exchange_weak(prev, curr, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    // Lost the race to another thread, retry.
                    Err(found) => prev = found,
                }
            }

            active += 1;
        }

        // Release: If we don't free the list, release any access of the batch to the
        // thread that eventually will.
        //
        // Safety: Local batch pointers are valid until relamation.
        if unsafe { &*batch }
            .active
            .fetch_add(active, Ordering::Release)
            .wrapping_add(active)
            == 0
        {
            // Acquire: Ensure any access of objects in the batch happen-before we free it.
            atomic::fence(Ordering::Acquire);

            // Reset the batch.
            unsafe { *local_batch = LocalBatch::default() };

            // Safety: The reference count is zero, meaning that either no threads were
            // active or they have all already decremented the reference count.
            //
            // Additionally, the local batch has been reset and we are not holding on to any
            // mutable references, so any recursive calls to retire during
            // reclamation are valid.
            unsafe { Collector::free_batch(batch) }
            return;
        }

        // Reset the batch.
        unsafe { *local_batch = LocalBatch::default() };
    }

    /// Traverse the reservation list, decrementing the reference count of each
    /// batch.
    ///
    /// # Safety
    ///
    /// `list` must be a valid reservation list.
    #[cold]
    #[inline(never)]
    unsafe fn traverse(mut list: *mut Entry) {
        while !list.is_null() {
            let curr = list;

            // Advance the cursor.
            // Safety: `curr` is a valid, non-null node in the list.
            list = unsafe { (*(*curr).node).next };
            let batch = unsafe { (*curr).batch };

            // Safety: Batch pointers are valid for reads until they are freed.
            unsafe {
                // Release: If we don't free the list, release any access of objects in the
                // batch to the thread that will.
                if (*batch).active.fetch_sub(1, Ordering::Release) == 1 {
                    // Ensure any access of objects in the batch happen-before we free it.
                    atomic::fence(Ordering::Acquire);

                    // Safety: We have the last reference to the batch and it has been removed
                    // from our reservation list.
                    Collector::free_batch(batch)
                }
            }
        }
    }

    /// Reclaim all values in the collector, including recursive calls to
    /// retire.
    ///
    /// # Safety
    ///
    /// No threads may be accessing the collector or any values that have been
    /// retired.
    #[inline]
    pub unsafe fn reclaim_all(&self) {
        for local_batch in self.batches.iter() {
            let local_batch = local_batch.value.get();

            unsafe {
                let batch = (*local_batch).batch;

                // There is nothing to reclaim.
                if batch.is_null() {
                    continue;
                }

                // Tell any recursive calls to `retire` to reclaim immediately.
                (*local_batch).batch = LocalBatch::DROP;

                // Safety: we have `&mut self` and the batch is non-null.
                Collector::free_batch(batch);

                // Reset the batch.
                (*local_batch).batch = ptr::null_mut();
            }
        }
    }

    /// Free a batch of objects.
    ///
    /// # Safety
    ///
    /// The batch reference count must be zero. Additionally, the current thread
    /// must not be holding on to any mutable references to thread-locals,
    /// as recursive calls to retire may still access the local batch. The
    /// batch being retired must be unreachable through any recursive calls.
    #[inline]
    unsafe fn free_batch(batch: *mut Batch) {
        // Safety: We have the last reference to the batch.
        for entry in unsafe { (*batch).entries.iter_mut() } {
            unsafe { (entry.reclaim)(entry.node.cast::<Link>()) };
        }

        unsafe { LocalBatch::free(batch) };
    }
}

impl Drop for Collector {
    #[inline]
    fn drop(&mut self) {
        // Safety: We have `&mut self`.
        unsafe { self.reclaim_all() };
    }
}

/// A node attached to every allocated object.
///
/// Nodes keep track of their birth epoch and are also used
/// as links in thread-local reservation lists.
#[repr(C)]
pub union Node {
    // Before retiring: The epoch this node was created in.
    pub birth_epoch: u64,
    // While retiring: Temporary location for an active reservation list.
    head: *const AtomicPtr<Entry>,
    // After retiring: next node in the thread's reservation list
    next: *mut Entry,
    // In deferred batch: next node in the batch
    pub next_batch: *mut Node,
}

/// A per-thread reservation list.
///
/// Reservation lists are lists of retired entries, where each entry represents
/// a batch.
#[repr(C)]
pub struct Reservation {
    /// The head of the list
    head: AtomicPtr<Entry>,
    /// The epoch this thread last accessed a pointer in.
    epoch: AtomicU64,
    /// The number of active guards for this thread.
    pub guards: Cell<u64>,
    /// A lock used for owned guards to prevent concurrent operations.
    pub lock: Mutex<()>,
    /// The number of nodes allocated by this thread.
    node_count: Cell<u64>,
}

impl Default for Reservation {
    fn default() -> Self {
        Reservation {
            head: AtomicPtr::new(Entry::INACTIVE),
            epoch: AtomicU64::new(0),
            guards: Cell::new(0),
            lock: Mutex::new(()),
            node_count: Cell::new(0),
        }
    }
}

/// A batch of nodes waiting to be retired.
struct Batch {
    /// Nodes in this batch.
    ///
    /// TODO: This allocation could be flattened.
    entries: Vec<Entry>,
    /// The minimum epoch of all nodes in this batch.
    min_epoch: u64,
    /// The reference count for any active threads.
    active: AtomicUsize,
}

impl Batch {
    /// Create a new batch with the specified capacity.
    #[inline]
    fn new(capacity: usize) -> Batch {
        Batch {
            entries: Vec::with_capacity(capacity),
            min_epoch: 0,
            active: AtomicUsize::new(0),
        }
    }
}

/// A retired object.
struct Entry {
    /// Object metadata.
    node: *mut Node,
    /// The function used to reclaim this object.
    reclaim: unsafe fn(*mut Link),
    /// The batch this node is a part of.
    batch: *mut Batch,
}

impl Entry {
    /// Represents an inactive thread.
    ///
    /// While null indicates an empty list, INACTIVE indicates the thread has no
    /// active guards and is not accessing any objects.
    pub const INACTIVE: *mut Entry = usize::MAX as _;
}

/// A pointer to a batch, unique to the current thread.
pub struct LocalBatch {
    batch: *mut Batch,
}

impl Default for LocalBatch {
    fn default() -> Self {
        LocalBatch {
            batch: ptr::null_mut(),
        }
    }
}

impl LocalBatch {
    /// This is set during a call to reclaim_all, signalling recursive calls to
    /// retire to reclaim immediately.
    const DROP: *mut Batch = usize::MAX as _;

    /// Return a pointer to the batch, initializing it if the batch was null.
    #[inline]
    fn get_or_init(&mut self, capacity: usize) -> *mut Batch {
        if self.batch.is_null() {
            self.batch = Box::into_raw(Box::new(Batch::new(capacity)));
        }

        self.batch
    }

    /// Free the batch.
    #[inline]
    unsafe fn free(ptr: *mut Batch) {
        unsafe { drop(Box::from_raw(ptr)) }
    }
}

/// Local batches are only accessed by the current thread.
unsafe impl Send for LocalBatch {}
unsafe impl Sync for LocalBatch {}
