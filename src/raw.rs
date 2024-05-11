use crate::tls::{Thread, ThreadLocal};
use crate::utils::CachePadded;
use crate::{AsLink, Deferred, Link};

use std::cell::{Cell, UnsafeCell};
use std::num::NonZeroU64;
use std::ptr::{self, NonNull};
use std::sync::atomic::{self, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

// Fast, lock-free, robust concurrent memory reclamation.
//
// The core algorithm is described [in this paper](https://arxiv.org/pdf/2108.02763.pdf).
pub struct Collector {
    // Per-thread reservations lists
    reservations: ThreadLocal<CachePadded<Reservation>>,
    // Per-thread batches of retired nodes
    batches: ThreadLocal<CachePadded<UnsafeCell<LocalBatch>>>,
    // The number of nodes allocated per-thread
    node_count: ThreadLocal<UnsafeCell<u64>>,

    // The global epoch value
    pub(crate) epoch: AtomicU64,
    // The number of node allocations before advancing the global epoch
    pub(crate) epoch_frequency: Option<NonZeroU64>,
    // The number of nodes in a batch before we free
    pub(crate) batch_size: usize,
}

impl Collector {
    // Create a collector with the provided configuration.
    pub fn new(threads: usize, epoch_frequency: NonZeroU64, batch_size: usize) -> Self {
        Self {
            epoch: AtomicU64::new(1),
            reservations: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            node_count: ThreadLocal::with_capacity(threads),
            epoch_frequency: Some(epoch_frequency),
            batch_size,
        }
    }

    // Create a new node.
    pub fn node(&self) -> Node {
        // safety: node counts are only accessed by the current thread
        let count = unsafe { &mut *self.node_count.load(Thread::current()).get() };
        *count += 1;

        // record the current epoch value
        //
        // note that it's fine if we see older epoch values here, which just means more
        // threads will be counted as active than might actually be
        let birth_epoch = match self.epoch_frequency {
            // advance the global epoch
            Some(ref freq) if *count % freq.get() == 0 => {
                self.epoch.fetch_add(1, Ordering::Relaxed) + 1
            }
            Some(_) => self.epoch.load(Ordering::Relaxed),
            // we aren't tracking epochs
            None => 0,
        };

        Node { birth_epoch }
    }

    // Return the reservation for the current thread.
    //
    // # Safety
    //
    // The reservation must be accessed soundly across multiple threads.
    pub unsafe fn reservation(&self, thread: Thread) -> &Reservation {
        self.reservations.load(thread)
    }

    // Mark the current thread as active.
    //
    // `enter` and `leave` calls maintain a reference count to allow re-entrancy.
    // If the current thread is already marked as active, this method simply increases
    // the reference count.
    //
    // # Safety
    //
    // This method is not safe to call concurrently on the same thread.
    // This method must only be called if the current thread is inactive.
    pub unsafe fn enter(&self, reservation: &Reservation) {
        // mark the thread as active
        //
        // seqcst: establish a total order between this store and the fence in `retire`
        // - if our store comes first, the thread retiring will see that we are active
        // - if the fence comes first, we will see the new values of any objects being
        //   retired by that thread (all pointer loads are also seqcst and thus participate
        //   in the total order)
        reservation.head.store(ptr::null_mut(), Ordering::SeqCst);
    }

    // Load an atomic pointer.
    //
    // # Safety
    //
    // This method should only ever be called from the same thread.
    #[inline]
    pub unsafe fn protect_local<T>(
        &self,
        ptr: &AtomicPtr<T>,
        _ordering: Ordering,
        thread: Thread,
    ) -> *mut T {
        if self.epoch_frequency.is_none() {
            // epoch tracking is disabled, but pointer loads still need to be seqcst to participate
            // in the total order. see `enter` for details
            return ptr.load(Ordering::SeqCst);
        }

        let reservation = self.reservations.load(thread);

        // load the last epoch we recorded
        //
        // relaxed: the reservation epoch is only modified by the current thread
        let mut prev_epoch = reservation.epoch.load(Ordering::Relaxed);

        loop {
            // seqcst:
            // - ensure that this load participates in the total order. see the store
            //   to reservation.head and reservation.epoch for details
            // - acquire the birth epoch of the pointer. we need to record at least
            //   that epoch below to let other threads know we have access to this pointer
            //   this requires the pointer to have been stored with release ordering which
            //   is technically undocumented, but if it was not the resulting pointer would
            //   be unusable anyways
            let ptr = ptr.load(Ordering::SeqCst);

            // relaxed: we acquired at least the pointer's birth epoch above
            let current_epoch = self.epoch.load(Ordering::Relaxed);

            // we were already marked as active in the birth epoch, so we are safe
            if prev_epoch == current_epoch {
                return ptr;
            }

            // our epoch is out of date, record the new one and try again
            //
            // seqcst: establish a total order between this store and the fence in `retire`
            // - if our store comes first, the thread retiring will see that we are active in
            //   the current epoch
            // - if the fence comes first, we will see the new values of any objects being
            //   retired by that thread (all pointer loads are also seqcst and thus participate
            //   in the total order)
            reservation.epoch.store(current_epoch, Ordering::SeqCst);
            prev_epoch = current_epoch;
        }
    }

    // Load an atomic pointer.
    //
    // This method is safe to call concurrently with the same `thread`.
    #[inline]
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, _ordering: Ordering, thread: Thread) -> *mut T {
        if self.epoch_frequency.is_none() {
            // epoch tracking is disabled, but pointer loads still need to be seqcst to participate
            // in the total order. see `enter` for details
            return ptr.load(Ordering::SeqCst);
        }

        let reservation = self.reservations.load(thread);

        // load the last epoch we recorded
        //
        // acquire the global epoch if this was modified by another thread
        let mut prev_epoch = reservation.epoch.load(Ordering::Acquire);

        loop {
            // seqcst:
            // - ensure that this load participates in the total order. see the store
            //   to reservation.head and reservation.epoch for details
            // - acquire the birth epoch of the pointer. we need to record at least
            //   that epoch below to let other threads know we have access to this pointer.
            let ptr = ptr.load(Ordering::SeqCst);

            // relaxed: we acquired at least the pointer's birth epoch above
            let current_epoch = self.epoch.load(Ordering::Relaxed);

            // we were already marked as active in the birth epoch, so we are safe
            if prev_epoch == current_epoch {
                return ptr;
            }

            // our epoch is out of date, record the new one and try again.
            // note that this may be called concurrently so we need to ensure
            // we do not overwrite a later epoch.
            //
            // seqcst: establish a total order between this store and the fence in `retire`
            // - if our store comes first, the thread retiring will see that we are active in
            //   the current epoch
            // - if the fence comes first, we will see the new values of any objects being
            //   retired by that thread (all pointer loads are also seqcst and thus participate
            //   in the total order)
            // we also acquire the global epoch if the local epoch is concurrently modified
            // by another thread
            prev_epoch = reservation
                .epoch
                .fetch_max(current_epoch, Ordering::SeqCst)
                .max(current_epoch);
        }
    }

    // Mark the current thread as inactive.
    //
    // # Safety
    //
    // This method is not safe to call concurrently on the same thread.
    // Any previously protected pointers may be invalidated.
    pub unsafe fn leave(&self, reservation: &Reservation) {
        // release: exit the critical section
        let head = reservation.head.swap(Entry::INACTIVE, Ordering::Release);

        if head != Entry::INACTIVE {
            // acquire: acquire any new entries in the reservation list and the new values
            // of any objects that were retired
            atomic::fence(Ordering::Acquire);

            // decrement the reference counts of any entries that were added
            unsafe { Collector::traverse(head) }
        }
    }

    // Decrement any reference counts, keeping the thread marked as active.
    //
    // # Safety
    //
    // This method is not safe to call concurrently on the same thread.
    // Any previously protected values may be invalidated.
    pub unsafe fn refresh(&self, reservation: &Reservation) {
        // release: exit the critical section
        // seqcst: establish the same total order as in `enter`
        let head = reservation.head.swap(ptr::null_mut(), Ordering::SeqCst);

        if head != Entry::INACTIVE {
            // decrement the reference counts of any entries that were added
            unsafe { Collector::traverse(head) }
        }
    }

    // Add a node to the retirement batch, retiring the batch if `batch_size` is reached.
    //
    // # Safety
    //
    // `ptr` must be a valid pointer that is no longer accessible to any inactive threads.
    // This method is not safe to call concurrently with the same `thread`.
    pub unsafe fn add<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link), thread: Thread)
    where
        T: AsLink,
    {
        // safety: local batches are only accessed by the current thread until retirement
        let local_batch = unsafe {
            &mut *self
                .batches
                .load_or(|| LocalBatch::new(self.batch_size), thread)
                .get()
        };

        // safety: local batch pointers are always valid until reclamation
        let batch = unsafe { local_batch.0.as_mut() };

        // `ptr` is guaranteed to be a valid pointer that can be cast to a node (`T: AsLink`)
        //
        // any other thread with a reference to the pointer only has a shared
        // reference to the UnsafeCell<Node>, which is allowed to alias. the caller
        // guarantees that the same pointer is not retired twice, so we can safely write
        // to the node through this pointer.
        let node = UnsafeCell::raw_get(ptr.cast::<UnsafeCell<Node>>());

        // keep track of the oldest node in the batch
        //
        // if epoch tracking is disabled this will always be false (0 > 0).
        let birth_epoch = unsafe { (*node).birth_epoch };
        batch.min_epoch = batch.min_epoch.min(birth_epoch);

        // create an entry for this node
        batch.entries.push(Entry {
            node,
            reclaim,
            batch: local_batch.0.as_ptr(),
        });

        // attempt to retire the batch if we have enough entries
        if batch.entries.len() % self.batch_size == 0 {
            unsafe { self.try_retire(local_batch, thread) }
        }
    }

    // Retire a batch of nodes.
    //
    // # Safety
    //
    // The batch must no longer accessible to any inactive threads.
    // This method is not safe to call concurrently with the same `thread`.
    pub unsafe fn add_batch(
        &self,
        deferred: &mut Deferred,
        reclaim: unsafe fn(*mut Link),
        thread: Thread,
    ) {
        // safety: local batches are only accessed by the current thread until retirement
        let local_batch = unsafe {
            &mut *self
                .batches
                .load_or(|| LocalBatch::new(self.batch_size), thread)
                .get()
        };

        // safety: local batch pointers are always valid until reclamation
        let batch = unsafe { local_batch.0.as_mut() };

        // keep track of the oldest node in the batch
        let min_epoch = *deferred.min_epoch.get_mut();
        batch.min_epoch = batch.min_epoch.min(min_epoch);

        // we only want to keep retirement amortized consistently, so only retire if we
        // hit a multiple of the batch size
        let mut should_retire = false;

        deferred.for_each(|node| {
            // create an entry for this node
            batch.entries.push(Entry {
                node,
                reclaim,
                batch: local_batch.0.as_ptr(),
            });

            should_retire = should_retire || batch.entries.len() % self.batch_size == 0;
        });

        // attempt to retire the batch if we got enough entries
        if should_retire {
            unsafe { self.try_retire(local_batch, thread) }
        }
    }

    // Attempt to retire nodes in the current thread's batch.
    //
    // # Safety
    //
    // This method is not safe to call concurrently with the same `thread`.
    pub unsafe fn try_retire_batch(&self, thread: Thread) {
        let local_batch = self
            .batches
            .load_or(|| LocalBatch::new(self.batch_size), thread);

        // safety: batches are only accessed by the current thread
        unsafe { self.try_retire(&mut *local_batch.get(), thread) }
    }

    // Attempt to retire nodes in this batch.
    //
    // Note that if a guard on the current thread is active, the batch will also be added to it's
    // reservation list for deferred reclamation.
    //
    // # Safety
    //
    // This method is not safe to call concurrently with the same `thread`.
    pub unsafe fn try_retire(&self, local_batch: &mut LocalBatch, thread: Thread) {
        // establish a total order between the retirement of nodes in this batch and stores
        // marking a thread as active (or active in an epoch):
        // - if the store comes first, we will see that the thread is active
        // - if this fence comes first, the thread will see the new values of any objects
        //   in this batch.
        //
        // this fence also establishes synchronizes with the fence run when a thread is created:
        // - if our fence comes first, they will see the new values of any objects in this batch
        // - if their fence comes first, we will see the new thread
        atomic::fence(Ordering::SeqCst);

        // safety: local batch pointers are always valid until reclamation.
        // if the batch ends up being retired then this pointer is stable
        let batch_entries = unsafe { local_batch.0.as_mut().entries.as_mut_ptr() };
        let batch = unsafe { local_batch.0.as_ref() };

        // if there are not enough entries in this batch for active threads, we have to try again later
        //
        // relaxed: the fence above already ensures that we see any threads that might
        // have access to any objects in this batch. any other threads that were created
        // after it will see their new values.
        if batch.entries.len() <= self.reservations.threads.load(Ordering::Relaxed) {
            return;
        }

        let current_reservation = self.reservations.load(thread);
        let mut marked = 0;

        // record all active threads, including the current thread
        //
        // we need to do this in a separate step before actually retiring to
        // make sure we have enough entries, as the number of threads can grow
        for reservation in self.reservations.iter() {
            // if we don't have enough entries to insert into the reservation lists
            // of all active threads, try again later
            let Some(entry) = batch.entries.get(marked) else {
                return;
            };

            // if this thread is inactive, we can skip it
            //
            // relaxed: see the acquire fence below
            if reservation.head.load(Ordering::Relaxed) == Entry::INACTIVE {
                continue;
            }

            // if this thread's epoch is behind the earliest birth epoch in this batch
            // we can skip it, as there is no way it could have accessed any of the objects
            // in this batch. we make sure never to skip the current thread even if it's epoch
            // is behind because it may still have access to the pointer (because it's the
            // thread that allocated it). the current thread is only skipped if there is no
            // active guard.
            //
            // relaxed: if the epoch is behind there is nothing to synchronize with, and
            // we already ensured we will see it's relevant epoch with the seqcst fence
            // above
            //
            // if epoch tracking is disabled this is always false (0 < 0)
            if !ptr::eq(reservation, current_reservation)
                && reservation.epoch.load(Ordering::Relaxed) < batch.min_epoch
            {
                continue;
            }

            // temporarily store this thread's list in a node in our batch
            //
            // safety: all nodes in a batch are valid, and this batch has not been
            // shared yet to other threads
            unsafe { (*entry.node).head = &reservation.head }
            marked += 1;
        }

        // for any inactive threads we skipped above, synchronize with `leave` to ensure
        // any accesses happen-before we retire. we ensured with the seqcst fence above
        // that the next time the thread becomes active it will see the new values of any
        // objects in this batch
        atomic::fence(Ordering::Acquire);

        // add the batch to all active thread's reservation lists
        let mut active = 0;
        'retire: for i in 0..marked {
            let curr = &batch.entries[i];
            let curr_ptr = unsafe { batch_entries.add(i) };

            // safety: all nodes in the batch are valid, and we just initialized `head`
            // for all `marked` nodes in the loop above
            let head = unsafe { &*(*curr.node).head };

            // relaxed: we never access the head
            let mut prev = head.load(Ordering::Relaxed);

            loop {
                // the thread became inactive, skip it
                //
                // as long as the thread became inactive at some point after we verified it was
                // active, it can no longer access any objects in this batch. the next time it
                // becomes active it will load the new object values due to the seqcst fence above
                if prev == Entry::INACTIVE {
                    // acquire: synchronize with `leave` to ensure any accesses happen-before we retire
                    atomic::fence(Ordering::Acquire);
                    continue 'retire;
                }

                // link this node to the reservation list
                unsafe { (*curr.node).next = prev }

                // release: release the node and entries in this batch
                match head.compare_exchange_weak(
                    prev,
                    curr_ptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    // lost the race to another thread, retry
                    Err(found) => prev = found,
                }
            }

            active += 1;
        }

        // release: if we don't free the list, release any access of the batch to the thread that does
        if batch
            .active
            .fetch_add(active, Ordering::Release)
            .wrapping_add(active)
            == 0
        {
            // ensure any access of objects in the batch happen-before we free it
            atomic::fence(Ordering::Acquire);

            // safety: the reference count is 0, meaning that either no threads were active,
            // or they have all already decremented the reference count
            unsafe { Collector::free_batch(local_batch.0.as_ptr()) }
        }

        // reset the batch
        *local_batch = LocalBatch::new(self.batch_size).value.into_inner();
    }

    // Traverse the reservation list, decrementing the reference count of each batch.
    //
    // # Safety
    //
    // `list` must be a valid reservation list
    unsafe fn traverse(mut list: *mut Entry) {
        while !list.is_null() {
            let curr = list;

            // safety: `curr` is a valid non-null node in the list
            list = unsafe { (*(*curr).node).next };
            let batch = unsafe { (*curr).batch };

            // safety: batch pointers are valid for reads until they are freed
            unsafe {
                // release: if we don't free the list, release any access of objects in the batch
                // to the thread that will
                if (*batch).active.fetch_sub(1, Ordering::Release) == 1 {
                    // ensure any access of objects in the batch happen-before we free it
                    atomic::fence(Ordering::Acquire);

                    // safety: we have the last reference to the batch
                    Collector::free_batch(batch)
                }
            }
        }
    }

    // Free a reservation list.
    //
    // # Safety
    //
    // The batch reference count must be zero.
    unsafe fn free_batch(batch: *mut Batch) {
        // safety: we are the last reference to the batch
        for entry in unsafe { (*batch).entries.iter_mut() } {
            unsafe { (entry.reclaim)(entry.node.cast::<Link>()) };
        }

        unsafe { LocalBatch::free(batch) };
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        for batch in self.batches.iter() {
            // safety: we have &mut self
            let batch = unsafe { &mut *batch.get() };

            // safety: we have &mut self
            unsafe { Collector::free_batch(batch.0.as_ptr()) }
        }
    }
}

// A node attached to every allocated object.
//
// Nodes keep track of their birth epoch, as well as thread-local
// reservation lists.
#[repr(C)]
pub union Node {
    // Before retiring: the epoch this node was created in
    pub birth_epoch: u64,
    // While retiring: temporary location for an active reservation list.
    head: *const AtomicPtr<Entry>,
    // After retiring: next node in the thread's reservation list
    next: *mut Entry,
    // In deferred batch: next node in the batch
    pub next_batch: *mut Node,
}

// A per-thread reservation list.
//
// Reservation lists are lists of retired entries, where
// each entry represents a batch.
#[repr(C)]
pub struct Reservation {
    // The head of the list
    head: AtomicPtr<Entry>,
    // The epoch this thread last accessed a pointer in
    epoch: AtomicU64,
    // the number of active guards for this thread
    pub guards: Cell<u64>,
    pub lock: Mutex<()>,
}

impl Default for Reservation {
    fn default() -> Self {
        Reservation {
            head: AtomicPtr::new(Entry::INACTIVE),
            epoch: AtomicU64::new(0),
            guards: Cell::new(0),
            lock: Mutex::new(()),
        }
    }
}

// A batch of nodes waiting to be retired
struct Batch {
    // Nodes in this batch.
    //
    // TODO: this allocation can be flattened
    entries: Vec<Entry>,
    // The minimum epoch of all nodes in this batch.
    min_epoch: u64,
    // The reference count for active threads.
    active: AtomicUsize,
}

// A retired node.
struct Entry {
    node: *mut Node,
    reclaim: unsafe fn(*mut Link),
    // the batch this node is a part of.
    batch: *mut Batch,
}

impl Entry {
    // Represents an inactive thread.
    //
    // While null indicates an empty list, INACTIVE indicates the thread has no active
    // guards and is not accessing any objects.
    pub const INACTIVE: *mut Entry = -1_isize as usize as _;
}

pub struct LocalBatch(NonNull<Batch>);

impl LocalBatch {
    // Create a new batch with an initial capacity.
    fn new(capacity: usize) -> CachePadded<UnsafeCell<LocalBatch>> {
        let ptr = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Batch {
                entries: Vec::with_capacity(capacity),
                min_epoch: 0,
                active: AtomicUsize::new(0),
            })))
        };

        CachePadded::new(UnsafeCell::new(LocalBatch(ptr)))
    }

    // Free the batch.
    unsafe fn free(ptr: *mut Batch) {
        unsafe { drop(Box::from_raw(ptr)) }
    }
}

// Local batches are only accessed by the current thread.
unsafe impl Send for LocalBatch {}
unsafe impl Sync for LocalBatch {}
