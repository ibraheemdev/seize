use crate::tls::ThreadLocal;
use crate::utils::CachePadded;
use crate::{Link, Linked};

use std::cell::{Cell, UnsafeCell};
use std::mem::ManuallyDrop;
use std::num::NonZeroU64;
use std::ptr::{self, NonNull};
use std::sync::atomic::{self, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// Fast, lock-free, robust concurrent memory reclamation.
//
// The core algorithm is described [in this paper](https://arxiv.org/pdf/2108.02763.pdf).
pub struct Collector {
    // The global epoch value
    pub(crate) epoch: AtomicU64,
    // Per-thread reservations lists
    reservations: ThreadLocal<CachePadded<Reservation>>,
    // Per-thread batches of retired nodes
    batches: ThreadLocal<UnsafeCell<CachePadded<Batch>>>,
    // The number of nodes allocated per-thread
    node_count: ThreadLocal<UnsafeCell<u64>>,
    // The number of node allocations before advancing the global epoch
    pub(crate) epoch_frequency: Option<NonZeroU64>,
    // The number of nodes in a batch before we free
    pub(crate) batch_size: usize,
}

impl Collector {
    pub fn with_threads(threads: usize, epoch_frequency: NonZeroU64, batch_size: usize) -> Self {
        Self {
            epoch: AtomicU64::new(1),
            reservations: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            node_count: ThreadLocal::with_capacity(threads),
            epoch_frequency: Some(epoch_frequency),
            batch_size,
        }
    }

    // Create a new node
    pub fn node(&self) -> Node {
        // safety: node counts are only accessed by the current thread
        let count = unsafe { &mut *self.node_count.get_or(Default::default).get() };
        *count += 1;

        // record the current epoch value
        //
        // note that it's fine if we see older epoch values here,
        // which just means more threads will be counted as active
        // than might actually be
        let birth_epoch = match self.epoch_frequency {
            // advance the global epoch
            Some(ref freq) if *count % freq.get() == 0 => {
                let epoch = self.epoch.fetch_add(1, Ordering::Relaxed);
                epoch + 1
            }
            Some(_) => self.epoch.load(Ordering::Relaxed),
            // we aren't tracking epochs
            None => 0,
        };

        Node {
            batch: ptr::null_mut(),
            reservation: ReservationNode { birth_epoch },
        }
    }

    // Mark the current thread as active
    pub fn enter(&self) {
        let reservation = self.reservations.get_or(Default::default);

        // calls to `enter` may be reentrant, so we need to keep track of
        // the number of active guards for the current thread
        let guards = reservation.guards.get();
        reservation.guards.set(guards + 1);

        // avoid clearing already active reservation lists
        if guards == 0 {
            // mark the thread as active
            //
            // seqcst: establish a total order between this store and the fence in `retire`
            // - if our store comes first, the thread retiring will see that we are active
            // - if the fence comes first, we will see the new values of any pointers being
            //   retired by that thread (all pointer loads are also seqcst and thus participate
            //   in the total order)
            reservation.head.store(ptr::null_mut(), Ordering::SeqCst);
        }
    }

    // Load an atomic pointer
    #[inline]
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, _ordering: Ordering) -> *mut T {
        if self.epoch_frequency.is_none() {
            // epoch tracking is disabled, but pointer loads still need to be seqcst to participate
            // in the total order. see `enter` for details
            return ptr.load(Ordering::SeqCst);
        }

        let reservation = self.reservations.get_or(Default::default);

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
            // - if the fence comes first, we will see the new values of any pointers being
            //   retired by that thread (all pointer loads are also seqcst and thus participate
            //   in the total order)
            reservation.epoch.store(current_epoch, Ordering::SeqCst);
            prev_epoch = current_epoch;
        }
    }

    // Mark the current thread as inactive
    pub fn leave(&self) {
        let reservation = self.reservations.get_or(Default::default);

        // decrement the active guard count
        let guards = reservation.guards.get();
        reservation.guards.set(guards - 1);

        // we can only decrement reference counts after all guards for
        // the current thread are dropped
        if guards == 1 {
            // release: exit the critical section
            // acquire: acquire any new reservation nodes
            let head = reservation.head.swap(Node::INACTIVE, Ordering::AcqRel);

            if head != Node::INACTIVE {
                // decrement the reference counts of any nodes that were added
                unsafe { Collector::traverse(head) }
            }
        }
    }

    // Decrement any reference counts, keeping the thread marked as active
    pub unsafe fn flush(&self) {
        let reservation = self.reservations.get_or(Default::default);
        let guards = reservation.guards.get();

        // we can only decrement reference counts after all guards for
        // the current thread are dropped
        if guards == 1 {
            // release: exit the critical section
            // acquire: acquire any new reservation nodes
            let head = reservation.head.swap(ptr::null_mut(), Ordering::AcqRel);

            if head != Node::INACTIVE {
                // decrement the reference counts of any nodes that were added
                unsafe { Collector::traverse(head) }
            }
        }
    }

    // Add a node to the retirement batch.
    //
    // Returns `true` if the batch size has been reached and the batch should be retired.
    pub unsafe fn add<T>(
        &self,
        ptr: *mut Linked<T>,
        reclaim: unsafe fn(Link),
    ) -> (bool, &mut Batch) {
        // safety: batches are only accessed by the current thread
        let batch = unsafe { &mut *self.batches.get_or(|| Batch::new(self.batch_size)).get() };

        let node = UnsafeCell::raw_get(ptr::addr_of_mut!((*ptr).node));

        // safety: `ptr` is guaranteed to be a valid pointer
        //
        // any other thread with a reference to the pointer only has a shared
        // reference to the UnsafeCell<Node>, which is allowed to alias. the caller
        // guarantees that the same pointer is not retired twice, so we can safely write
        // to the node here
        unsafe { (*node).batch = batch.ptr.as_ptr() }

        // the TAIL node stores the minimum epoch of the batch. if a thread is active
        // in the minimum birth era, it has access to at least one of the nodes in the
        // batch and must be tracked.
        //
        // if epoch tracking is disabled this will always be false (0 > 0).
        if batch.ptr.as_ref().min_epoch > (*node).reservation.birth_epoch {
            batch.ptr.as_mut().min_epoch = (*node).reservation.birth_epoch;
        }

        // add the node to the list
        batch.ptr.as_mut().entries.push(Entry { node, reclaim });

        (
            batch.ptr.as_mut().entries.len() % self.batch_size == 0,
            batch,
        )
    }

    // Attempt to retire nodes in the current thread's batch
    //
    // # Safety
    //
    // The batch must contain at least one node
    pub unsafe fn retire_batch(&self) {
        // safety: batches are only accessed by the current thread
        let batch = unsafe { &mut *self.batches.get_or(|| Batch::new(self.batch_size)).get() };
        // safety: upheld by caller
        unsafe { self.retire(batch) }
    }

    // Attempt to retire nodes in this batch
    //
    // # Safety
    //
    // The batch must contain at least one node
    pub unsafe fn retire(&self, batch: &mut Batch) {
        // establish a total order between the retirement of nodes in this batch, and stores
        // marking a thread as active:
        // - if this fence comes first, we will see that the thread is active
        // - if the store comes first, the thread will see the new values of any pointers
        //   in this batch
        //
        // this fence also establishes synchronizes with the fence run when a thread is created:
        // - if our fence comes first, they will see the new values of any pointers in this batch
        // - if their fence comes first, we will see the new thread
        atomic::fence(Ordering::SeqCst);

        // if there are not enough nodes in this batch for active threads, we have to try again later
        //
        // relaxed: the fence above already ensures that we see any threads that might
        // have access to any pointers in this batch. any other threads that were created
        // after it will see their new values.
        if batch.ptr.as_ref().entries.len() <= self.reservations.threads.load(Ordering::Relaxed) {
            return;
        }

        let mut marked = 0;

        // record all active threads
        //
        // we need to do this in a separate step before actually retiring to
        // make sure we have enough reservation nodes, as the number of threads can grow
        dbg!(self.reservations.threads.load(Ordering::Relaxed));
        for reservation in self.reservations.iter() {
            let node = batch.ptr.as_ref().entries[marked].node;

            // if this thread is inactive, we can skip it
            //
            // acquire: if the thread is inactive, synchronize with `leave`
            // to ensure any accesses happen-before we retire
            if reservation.head.load(Ordering::Acquire) == Node::INACTIVE {
                continue;
            }

            // if this thread's epoch is behind the earliest birth epoch
            // in this batch we can skip it, as there is no way it could
            // have accessed any of the pointers in this batch
            //
            // if epoch tracking is disabled this is always false (0 < 0)
            if reservation.epoch.load(Ordering::Acquire) < batch.ptr.as_ref().min_epoch {
                continue;
            }

            // we don't have enough nodes to insert into the reservation lists
            // of all active threads, try again later
            if marked == batch.ptr.as_ref().entries.len() - 1 {
                return;
            }

            // temporarily store this thread's list in a node in our batch
            //
            // safety: we checked that this is not the last node in the batch
            // list above, and all nodes in a batch are valid
            unsafe {
                (*node).reservation.head = &reservation.head;
                marked += 1;
            }
        }

        // acquire: for any inactive threads that we skipped above, synchronize with
        // the release store of INACTIVE in `leave`, or of the new epoch `protect`, to
        // ensure that any accesses of old pointer values happen-before we retire
        atomic::fence(Ordering::Acquire);

        // add the batch to all active thread's reservation lists
        let mut active = 0;
        let mut i = 0;

        while i < marked {
            let curr = batch.ptr.as_ref().entries[i].node;

            // safety: all nodes in the batch are valid, and we just initialized
            // `reservation.head` for all nodes until `last` in the loop above
            let head = unsafe { &*(*curr).reservation.head };

            // acquire:
            // - if the thread is inactive, synchronize with `leave` to ensure any
            //   accesses happen-before we retire
            // - if the thread is active, acquire any reservation nodes added by
            //   a concurrent call to `retire`
            let mut prev = head.load(Ordering::Acquire);

            loop {
                // the thread became inactive, skip it
                //
                // as long as the thread became inactive at some point after
                // we verified it was active, it can no longer access any pointers
                // in this batch. the next time it becomes active it will load
                // the new pointer values due to the seqcst fence above.
                if prev == Node::INACTIVE {
                    break;
                }

                // relaxed: acq/rel synchronization is provided by `head`
                unsafe { (*curr).reservation.next.store(prev, Ordering::Relaxed) }

                // release: release the new reservation nodes
                match head.compare_exchange_weak(prev, curr, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => {
                        active += 1;
                        break;
                    }
                    // lost the race to another thread, retry
                    Err(found) => {
                        // acquire the new reservation loads
                        atomic::fence(Ordering::Acquire);

                        prev = found;
                        continue;
                    }
                }
            }

            i += 1;
        }

        // release: if we don't free the list, release any modifications
        // of the data to the thread that will
        if batch
            .ptr
            .as_ref()
            .ref_count
            .fetch_add(active, Ordering::Release)
            .wrapping_add(active)
            == 0
        {
            // we are freeing the list, acquire any modifications of the data
            // released by the threads that decremented the count
            atomic::fence(Ordering::Acquire);

            // safety: The reference count is 0, meaning that either no threads
            // were active, or they have all already decremented the count
            unsafe { Collector::free_list(batch.ptr.as_ptr()) }
        }

        // reset the batch
        batch.ptr = BatchAlloc::alloc(self.batch_size);
    }

    // Traverse the reservation list, decrementing the
    // refernce count of each batch
    //
    // # Safety
    //
    // `list` must be a valid reservation list
    unsafe fn traverse(mut list: *mut Node) {
        loop {
            let curr = list;

            if curr.is_null() {
                break;
            }

            // safety: `curr` is a valid link in the list
            //
            // relaxed: any reservation nodes were acquired when we loaded `head`
            list = unsafe { (*curr).reservation.next.load(Ordering::Relaxed) };
            let batch = unsafe { (*curr).batch };

            // safety: TAIL nodes store the reference count of the batch
            //
            // acquire: if we free the list, acquire any
            // modifications to the data released by the threads
            // that decremented the count
            //
            // release: if we don't free the list, release any
            // modifications to the data to the thread that will
            unsafe {
                // release: if we don't free the list, release any modifications of
                // the data to the thread that will
                if (*batch).ref_count.fetch_sub(1, Ordering::Release) == 1 {
                    // we are freeing the list, acquire any modifications of the data
                    // released by the threads that decremented the count
                    atomic::fence(Ordering::Acquire);

                    // safety: we have the last reference to the batch
                    Collector::free_list(batch)
                }
            }
        }
    }

    // Free a reservation list
    //
    // # Safety
    //
    // Must have unique reference to the batch.
    unsafe fn free_list(batch: *mut BatchAlloc) {
        // safety: unique reference
        for entry in (*batch).entries.iter_mut() {
            (entry.reclaim)(Link { node: entry.node });
        }

        BatchAlloc::free(batch);
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        for batch in self.batches.iter() {
            // safety: We have &mut self
            let batch = unsafe { &mut *batch.get() };
            // safety: We have &mut self,
            unsafe { Collector::free_list(batch.ptr.as_ptr()) }
        }
    }
}

// A node is attached to every allocated object
//
// There are two types of nodes:
// - TAIL: the first node added to the batch (tail of the list), holds the reference count
// - SLOT: everyone else
pub struct Node {
    batch: *mut BatchAlloc,
    reservation: ReservationNode,
}

#[repr(C)]
union ReservationNode {
    // SLOT (after retiring): next node in the reservation list
    next: ManuallyDrop<AtomicPtr<Node>>,
    // SLOT (while retiring): temporary location for an active reservation list
    head: *const AtomicPtr<Node>,
    // TAIL: minimum epoch of the batch
    min_epoch: u64,
    // Before retiring: the epoch this node was created in
    birth_epoch: u64,
}

impl Node {
    // Represents an inactive thread
    //
    // While null indicates an empty list, INACTIVE indicates the thread has
    // no active guards and is not accessing any nodes.
    pub const INACTIVE: *mut Node = -1_isize as usize as _;
}

// A per-thread reservation list
#[repr(C)]
struct Reservation {
    // The head of the list
    head: AtomicPtr<Node>,
    // The epoch this thread last accessed a pointer in
    epoch: AtomicU64,
    // the number of active guards for this thread
    guards: Cell<u64>,
}

impl Default for Reservation {
    fn default() -> Self {
        Reservation {
            head: AtomicPtr::new(Node::INACTIVE),
            epoch: AtomicU64::new(0),
            guards: Cell::new(0),
        }
    }
}

// A batch of nodes waiting to be retired
pub struct Batch {
    ptr: NonNull<BatchAlloc>,
}

impl Batch {
    fn new(capacity: usize) -> UnsafeCell<CachePadded<Self>> {
        let ptr = unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(BatchAlloc {
                entries: Vec::with_capacity(capacity),
                min_epoch: 0,
                ref_count: AtomicUsize::new(0),
            })))
        };

        UnsafeCell::new(CachePadded::new(Batch { ptr }))
    }
}

// todo: flatten this allocation
struct BatchAlloc {
    entries: Vec<Entry>,
    min_epoch: u64,
    ref_count: AtomicUsize,
}

impl BatchAlloc {
    fn alloc(capacity: usize) -> NonNull<BatchAlloc> {
        unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(BatchAlloc {
                entries: Vec::with_capacity(capacity),
                min_epoch: 0,
                ref_count: AtomicUsize::new(0),
            })))
        }
    }

    unsafe fn free(ptr: *mut BatchAlloc) {
        unsafe { drop(Box::from_raw(ptr)) }
    }
}

struct Entry {
    node: *mut Node,
    reclaim: unsafe fn(Link),
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}
