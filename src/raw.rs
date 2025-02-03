use crate::membarrier;
use crate::tls::{Thread, ThreadLocal};
use crate::utils::CachePadded;

use std::cell::{Cell, UnsafeCell};
use std::ptr;
use std::sync::atomic::{self, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Fast, lock-free, and robust concurrent memory reclamation.
///
/// The core algorithm is described [in this paper](https://arxiv.org/pdf/2108.02763.pdf).
pub struct Collector {
    /// Per-thread batches of retired nodes.
    ///
    /// Retired values are added to thread-local batches before starting
    /// the reclamation process, amortizing the cost of retirement.
    batches: ThreadLocal<CachePadded<UnsafeCell<LocalBatch>>>,

    /// Per-thread reservations lists.
    ///
    /// A reservation list is a list of batches that were retired
    /// while the current thread was active. The thread must decrement
    /// the reference count and potentially free the batch of any
    /// reservations before exiting.
    reservations: ThreadLocal<CachePadded<Reservation>>,

    /// The minimum number of nodes required in a batch before freeing.
    pub(crate) batch_size: usize,
}

impl Collector {
    /// Create a collector with the provided configuration.
    pub fn new(threads: usize, batch_size: usize) -> Self {
        Self {
            reservations: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            batch_size: batch_size.next_power_of_two(),
        }
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
        reservation
            .head
            .store(ptr::null_mut(), membarrier::light_store());

        // This barrier, combined with the light store above, synchronizes with the heavy
        // barrier in `retire`:
        // - If our store comes first, the thread retiring will see that we are active.
        // - If the fence comes first, we will see the new values of any objects being
        //   retired by that thread
        //
        // Note that all pointer loads perform a light barrier to participate in the total order.
        membarrier::light_store_barrier();
    }

    /// Load an atomic pointer.
    #[inline]
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>) -> *mut T {
        // We have to respect both the user provided ordering and the ordering required
        // by the membarrier strategy. `SeqCst` is equivalent to `Acquire` on all (relevant)
        // platforms, so we just use it unconditionally.
        let value = ptr.load(Ordering::SeqCst);

        // The light barrier ensures that this load participates in the total order.
        // See `enter` for details.
        membarrier::light_load_barrier();

        return value;
    }

    /// Mark the current thread as inactive.
    ///
    /// # Safety
    ///
    /// Any previously protected pointers may be invalidated after calling
    /// `leave`. Additionally, this method is not safe to call concurrently
    /// with the same reservation.
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
    /// `leave`. Additionally, this method is not safe to call concurrently with
    /// the same reservation.
    #[inline]
    pub unsafe fn refresh(&self, reservation: &Reservation) {
        // SeqCst: Establish the ordering of a combined call to `leave` and `enter`.
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
    pub unsafe fn add<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut ()), thread: Thread) {
        let local_batch = self.batches.load(thread).get();

        // Safety: Local batches are only accessed by the current thread.
        let batch = unsafe { (*local_batch).get_or_init(self.batch_size) };

        // If we are in a recursive call during `drop` or `reclaim_all`, reclaim the
        // object immediately.
        if batch == LocalBatch::DROP {
            unsafe { reclaim(ptr.cast()) }
            return;
        }

        // Safety: The node and batch are both valid for mutable access.
        let len = unsafe {
            // Create an entry for this node.
            (*batch).entries.push(Entry {
                batch,
                reclaim,
                ptr: ptr.cast(),
                state: EntryState {
                    head: ptr::null_mut(),
                },
            });

            (*batch).entries.len()
        };

        // Attempt to retire the batch if we have enough entries.
        if len >= self.batch_size {
            // Safety: The caller guarantees that this method is not called concurrently
            // with the same `thread` and we are not holding on to any mutable references.
            unsafe { self.try_retire(local_batch) }
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

        // Safety: The caller guarantees this method is not called concurrently
        // with the same `thread`.
        unsafe { self.try_retire(local_batch) }
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
    pub unsafe fn try_retire(&self, local_batch: *mut LocalBatch) {
        // Establish a total order between the retirement of nodes in this batch and
        // light stores marking a thread as active:
        // - If the store comes first, we will see that the thread is active.
        // - If this barrier comes first, the thread will see the new values of any
        //   objects in this batch.
        //
        // This barrier also establishes synchronizes with the light store executed when a
        // thread is created:
        // - If our barrier comes first, they will see the new values of any objects in
        //   this batch.
        // - If their store comes first, we will see the new thread.
        membarrier::heavy();

        // Safety: Local batches are only accessed by the current thread.
        let batch = unsafe { (*local_batch).batch };

        // There is nothing to retire.
        if batch.is_null() || batch == LocalBatch::DROP {
            return;
        }

        let batch_entries = unsafe { (*batch).entries.as_mut_ptr() };

        let mut marked = 0;

        // Record all active threads, including the current thread.
        //
        // We need to do this in a separate step before actually retiring the batch to
        // ensure we have enough entries for reservation lists, as the number of threads
        // can grow dynamically.
        for reservation in self.reservations.iter() {
            // If this thread is inactive, we can skip it. The heavy barrier above
            // ensurse that the next time it becomes active, it will see the new
            // values of any objects in this batch.
            //
            // Relaxed: See the Acquire fence below.
            if reservation.head.load(Ordering::Relaxed) == Entry::INACTIVE {
                continue;
            }

            // If we don't have enough entries to insert into the reservation lists
            // of all active threads, try again later.
            //
            // Safety: Local batch pointers are valid until relamation.
            let Some(entry) = unsafe { &mut (*batch).entries }.get_mut(marked) else {
                return;
            };

            // Temporarily store this reservation list in the batch.
            //
            // Safety: All nodes in a batch are valid and this batch has not yet been shared
            // to other threads.
            entry.state.head = &reservation.head;
            marked += 1;
        }

        // For any inactive threads we skipped above, synchronize with `leave` to ensure
        // any accesses happen-before we retire. We ensured with the heavy barrier above
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
            let head = unsafe { &*(*curr).state.head };

            // Relaxed: All writes to the `head` use RMW instructions, so the previous node
            // in the list is synchronized through the release sequence on `head`.
            let mut prev = head.load(Ordering::Relaxed);

            loop {
                // The thread became inactive, skip it.
                //
                // As long as the thread became inactive at some point after the heavy barrier,
                // it can no longer access any objects in this batch. The next time it becomes
                // active it will load the new object values due to the heavy barrier above.
                if prev == Entry::INACTIVE {
                    // Acquire: Synchronize with `leave` to ensure any accesses happen-before we
                    // retire.
                    atomic::fence(Ordering::Acquire);
                    continue 'retire;
                }

                // Link this node to the reservation list.
                unsafe { (*curr).state.next = prev }

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
            // mutable references, so any recursive calls to retire during reclamation are valid.
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
            list = unsafe { (*curr).state.next };
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
            unsafe { (entry.reclaim)(entry.ptr.cast()) };
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

/// A per-thread reservation list.
///
/// Reservation lists are lists of retired entries, where each entry represents
/// a batch.
#[repr(C)]
pub struct Reservation {
    /// The head of the list
    head: AtomicPtr<Entry>,

    /// The number of active guards for this thread.
    pub guards: Cell<u64>,

    /// A lock used for owned guards to prevent concurrent operations.
    pub lock: Mutex<()>,
}

impl Default for Reservation {
    fn default() -> Self {
        Reservation {
            head: AtomicPtr::new(Entry::INACTIVE),
            guards: Cell::new(0),
            lock: Mutex::new(()),
        }
    }
}

/// A batch of nodes waiting to be retired.
struct Batch {
    /// Nodes in this batch.
    ///
    /// TODO: This allocation could be flattened.
    entries: Vec<Entry>,

    /// The reference count for any active threads.
    active: AtomicUsize,
}

impl Batch {
    /// Create a new batch with the specified capacity.
    #[inline]
    fn new(capacity: usize) -> Batch {
        Batch {
            entries: Vec::with_capacity(capacity),
            active: AtomicUsize::new(0),
        }
    }
}

/// A retired object.
struct Entry {
    /// The pointer to the object.
    ptr: *mut (),

    /// The function used to reclaim this object.
    reclaim: unsafe fn(*mut ()),

    /// The state of the retired object.
    state: EntryState,

    /// The batch that this node is a part of.
    batch: *mut Batch,
}

/// The state of a retired object.
#[repr(C)]
pub union EntryState {
    // While retiring: Temporary location for an active reservation list.
    head: *const AtomicPtr<Entry>,

    // After retiring: The next node in the thread's reservation list
    next: *mut Entry,
}

impl Entry {
    /// Represents an inactive thread.
    ///
    /// While null indicates an empty list, INACTIVE indicates the thread has no
    /// active guards and is not currently accessing any objects.
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
    /// This is set during a call to `reclaim_all`, signalling recursive calls to
    /// retire to reclaim immediately.
    const DROP: *mut Batch = usize::MAX as _;

    /// Returns a pointer to the batch, initializing the batch if it was null.
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

/// Safety: Local batches are only accessed by the current thread.
unsafe impl Send for LocalBatch {}
unsafe impl Sync for LocalBatch {}
