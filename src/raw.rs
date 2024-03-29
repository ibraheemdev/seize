use crate::tls::{Thread, ThreadLocal};
use crate::utils::CachePadded;
use crate::{AsLink, Link};

use std::cell::{Cell, UnsafeCell};
use std::mem::{self, ManuallyDrop};
use std::num::NonZeroU64;
use std::ptr;
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

        Node {
            reclaim: mem::drop,
            batch_link: ptr::null_mut(),
            reservation: ReservationNode { birth_epoch },
            batch: BatchNode {
                ref_count: ManuallyDrop::new(AtomicUsize::new(0)),
            },
        }
    }

    // Mark the current thread as active.
    //
    // # Safety
    //
    // `Thread` must be the current thread.
    pub unsafe fn enter(&self, thread: Thread) {
        let reservation = self.reservations.load(thread);

        // calls to `enter` may be reentrant, so we need to keep track of the number
        // of active guards for the current thread
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

    // Load an atomic pointer.
    //
    // # Safety
    //
    // `Thread` must be the current thread.
    #[inline]
    pub unsafe fn protect<T>(
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
            //   (TOOD: this requires pointers to be stored with release ordering, which is
            //   not documented)
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

    // Mark the current thread as inactive.
    //
    // # Safety
    //
    // `Thread` must be the current thread.
    pub unsafe fn leave(&self, thread: Thread) {
        let reservation = self.reservations.load(thread);

        // decrement the active guard count
        let guards = reservation.guards.get();
        reservation.guards.set(guards - 1);

        // we can only decrement reference counts after all guards for the current thread
        // are dropped
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

    // Decrement any reference counts, keeping the thread marked as active.
    pub unsafe fn refresh(&self, thread: Thread) {
        let reservation = self.reservations.load(thread);
        let guards = reservation.guards.get();

        // we can only decrement reference counts after all guards for the current
        // thread are dropped
        if guards == 1 {
            // release: exit the critical section
            // acquire: acquire any new reservation nodes and the values of any pointers
            // that were retired
            let head = reservation.head.swap(ptr::null_mut(), Ordering::AcqRel);

            if head != Node::INACTIVE {
                // decrement the reference counts of any nodes that were added
                unsafe { Collector::traverse(head) }
            }
        }
    }

    // Add a node to the retirement batch, retiring the batch if `batch_size` is reached.
    //
    // # Safety
    //
    // `ptr` is a valid pointer.
    pub unsafe fn add<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut Link), thread: Thread)
    where
        T: AsLink,
    {
        // safety: batches are only accessed by the current thread
        let batch = unsafe { &mut *self.batches.load(thread).get() };

        // safety: `ptr` is guaranteed to be a valid pointer that can be cast to a node
        let node = UnsafeCell::raw_get(ptr.cast::<UnsafeCell<Node>>());

        // any other thread with a reference to the pointer only has a shared
        // reference to the UnsafeCell<Node>, which is allowed to alias. the caller
        // guarantees that the same pointer is not retired twice, so we can safely write
        // to the node here
        unsafe { (*node).reclaim = reclaim }

        // add the node to the list
        if batch.head.is_null() {
            batch.tail = node;
            // implicit node.batch.ref_count = 0
        } else {
            // the TAIL node stores the minimum epoch of the batch. if a thread is active
            // in the minimum birth era, it has access to at least one of the nodes in the
            // batch and must be tracked.
            //
            // if epoch tracking is disabled this will always be false (0 > 0).
            unsafe {
                if (*batch.tail).reservation.min_epoch > (*node).reservation.birth_epoch {
                    (*batch.tail).reservation.min_epoch = (*node).reservation.birth_epoch;
                }
            }

            // safety: same as the write to `node` above
            unsafe {
                // the batch link of a SLOT node points to the tail
                (*node).batch_link = batch.tail;

                // insert this node into the batch
                (*node).batch.next = batch.head;
            }
        }

        batch.head = node;
        batch.size += 1;

        // attempt to retire the batch if we have enough nodes
        if batch.size % self.batch_size == 0 {
            unsafe { self.try_retire(batch, thread) };
        }
    }

    // Attempt to retire nodes in the current thread's batch.
    //
    // # Safety
    //
    // `Thread` must be the current thread.
    pub unsafe fn try_retire_batch(&self, thread: Thread) {
        // safety: batches are only accessed by the current thread
        unsafe { self.try_retire(&mut *self.batches.load(thread).get(), thread) }
    }

    // Attempt to retire nodes in this batch.
    //
    // Note that if a guard on the current thread is active, the batch will also be added to it's reservation
    // list for deferred reclamation.
    //
    // # Safety
    //
    // `Thread` must be the current thread.
    pub unsafe fn try_retire(&self, batch: &mut Batch, thread: Thread) {
        // establish a total order between the retirement of nodes in this batch and stores
        // marking a thread as active (or active in an epoch):
        // - if the store comes first, we will see that the thread is active
        // - if this fence comes first, the thread will see the new values of any pointers
        //   in this batch.
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
        if batch.head.is_null() || batch.size <= self.reservations.threads.load(Ordering::Relaxed) {
            return;
        }

        // safety: we made sure the batch is not empty
        unsafe { (*batch.tail).batch_link = batch.head }

        // safety: TAIL nodes always have `min_epoch` initialized
        let min_epoch = unsafe { (*batch.tail).reservation.min_epoch };
        let current_reservation = self.reservations.load(thread);

        let mut last = batch.head;

        // record all active threads
        //
        // we need to do this in a separate step before actually retiring to
        // make sure we have enough reservation nodes, as the number of threads can grow
        for reservation in self.reservations.iter() {
            // if this thread is inactive, we can skip it
            //
            // relaxed: see the acquire fence below
            if reservation.head.load(Ordering::Relaxed) == Node::INACTIVE {
                continue;
            }

            // if this thread's epoch is behind the earliest birth epoch in this batch
            // we can skip it, as there is no way it could have accessed any of the pointers
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
                && reservation.epoch.load(Ordering::Relaxed) < min_epoch
            {
                continue;
            }

            // we don't have enough nodes to insert into the reservation lists
            // of all active threads, try again later
            if last == batch.tail {
                return;
            }

            // temporarily store this thread's list in a node in our batch
            //
            // safety: we checked that this is not the last node in the batch list above,
            // and all nodes in a batch are valid
            unsafe {
                (*last).reservation.head = &reservation.head;
                last = (*last).batch.next;
            }
        }

        // for any inactive threads we skipped above, synchronize with `leave` to ensure
        // any accesses happen-before we retire. we ensured with the seqcst fence above
        // that the next time the thread becomes active it will see the new values of any
        // pointers in this batch.
        atomic::fence(Ordering::Acquire);

        // add the batch to all active thread's reservation lists
        let mut active = 0;
        let mut curr = batch.head;

        while curr != last {
            // safety: all nodes in the batch are valid, and we just initialized `reservation.head`
            // for all nodes until `last` in the loop above
            let head = unsafe { &*(*curr).reservation.head };

            // acquire:
            // - if the thread became inactive, synchronize with `leave` to ensure any accesses
            //   happen-before we retire
            // - if the thread is active, acquire any reservation nodes added by a concurrent call
            //   to `retire`
            let mut prev = head.load(Ordering::Acquire);

            loop {
                // the thread became inactive, skip it
                //
                // as long as the thread became inactive at some point after we verified it was
                // active, it can no longer access any pointers in this batch. the next time it
                // becomes active it will load the new pointer values due to the seqcst fence above
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

            curr = unsafe { (*curr).batch.next };
        }

        // safety: the TAIL node stores the reference count
        let ref_count = unsafe { &(*batch.tail).batch.ref_count };

        // release: if we don't free the list, release the batch to the thread that will
        if ref_count
            .fetch_add(active, Ordering::Release)
            .wrapping_add(active)
            == 0
        {
            // ensure any access of the data in the list happens-before we free the list
            atomic::fence(Ordering::Acquire);

            // safety: the reference count is 0, meaning that either no threads were active,
            // or they have all already decremented the count
            unsafe { Collector::free_list(batch.tail) }
        }

        // reset the batch
        batch.head = ptr::null_mut();
        batch.size = 0;
    }

    // Traverse the reservation list, decrementing the refernce count of each batch
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
            let tail = unsafe { (*curr).batch_link };

            // safety: TAIL nodes store the reference count of the batch
            unsafe {
                // release: if we don't free the list, release any access of the batch to the thread
                // that will
                if (*tail).batch.ref_count.fetch_sub(1, Ordering::Release) == 1 {
                    // ensure any access of the data in the list happens-before we free the list
                    atomic::fence(Ordering::Acquire);

                    // safety: we have the last reference to the batch
                    Collector::free_list(tail)
                }
            }
        }
    }

    // Free a reservation list.
    //
    // # Safety
    //
    // - `list` must be the last reference to the TAIL node of the batch.
    // - The reference count must be zero
    unsafe fn free_list(list: *mut Node) {
        // safety: `list` is a valid pointer
        let mut list = unsafe { (*list).batch_link };

        loop {
            let node = list;

            unsafe {
                list = (*node).batch.next;
                ((*node).reclaim)(node.cast::<Link>());
            }

            // if `node` is the TAIL node, then `node.batch.next` will interpret the
            // 0 in `node.batch.ref_count` as a null pointer, indicating that we have
            // freed the last node in the list
            if list.is_null() {
                break;
            }
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        for batch in self.batches.iter() {
            // safety: We have &mut self
            let batch = unsafe { &mut *batch.get() };

            if !batch.head.is_null() {
                // safety: batch.head is not null, meaning that `batch.tail` is valid
                unsafe {
                    // `free_list` expects the batch link to point to the head of the list
                    //
                    // usually this is done in `retire`
                    (*batch.tail).batch_link = batch.head;

                    // `free_list` expects the tail node's link to be null. usually this is
                    // implied by the reference count field in the union being zero, but that
                    // might not be the case here, so we have to set it manually
                    (*batch.tail).batch.next = ptr::null_mut();
                }

                // safety: We have &mut self
                unsafe { Collector::free_list(batch.tail) }
            }
        }
    }
}

// A node is attached to every allocated object
//
// There are two types of nodes:
// - TAIL: the first node added to the batch (tail of the list), holds the reference count
// - SLOT: everyone else
pub struct Node {
    // TAIL: pointer to the head of the list
    // SLOT: pointer to TAIL
    batch_link: *mut Node,
    // User provided drop glue
    reclaim: unsafe fn(*mut Link),
    // unions for different phases of a node's lifetime
    batch: BatchNode,
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

#[repr(C)]
union BatchNode {
    // SLOT: next node in the batch
    next: *mut Node,
    // TAIL: reference count of the batch
    ref_count: ManuallyDrop<AtomicUsize>,
}

impl Node {
    // Represents an inactive thread
    //
    // While null indicates an empty list, INACTIVE indicates the thread has no active
    // guards and is not accessing any nodes.
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
    // Head the batch
    head: *mut Node,
    // Tail of the batch (TAIL node)
    tail: *mut Node,
    // The number of nodes in this batch
    size: usize,
}

impl Default for Batch {
    fn default() -> Self {
        Batch {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            size: 0,
        }
    }
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}
