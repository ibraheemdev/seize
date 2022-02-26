use crate::cfg::trace;
use crate::tls::ThreadLocal;
use crate::utils::CachePadded;
use crate::{Link, Linked};

use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::num::NonZeroU64;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

// Fast, lock-free, robust concurrent memory reclamation.
//
// The core algorithm is described [in this paper](https://arxiv.org/pdf/2108.02763.pdf).
pub struct Collector {
    // The global epoch value.
    pub(crate) epoch: AtomicU64,
    // Per-thread reservations lists.
    reservations: ThreadLocal<CachePadded<Reservation>>,
    // Per-thread batches of retired nodes.
    batches: ThreadLocal<UnsafeCell<CachePadded<Batch>>>,
    // The number of nodes allocated per-thread.
    node_count: ThreadLocal<UnsafeCell<u64>>,
    // The number of node allocations before advancing the global epoch.
    pub(crate) epoch_frequency: Option<NonZeroU64>,
    // The number of nodes in a batch before we free.
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

    // Create a new node, storing the current epoch value.
    pub fn node(&self) -> Node {
        let count = self.node_count.get_or(Default::default).get();

        // SAFETY: node counts are only accessed by the current thread
        unsafe {
            *count += 1;
            trace!("allocated new value, values: {}", *count);

            if let Some(ref epoch_frequency) = self.epoch_frequency {
                if *count % epoch_frequency.get() == 0 {
                    // Advance the global epoch
                    //
                    // Like with most counters, this can be relaxed.
                    let _epoch = self.epoch.fetch_add(1, Ordering::Relaxed);
                    trace!("advancing global epoch to {}", _epoch + 1);
                }
            }
        }

        Node {
            reclaim: |_| {},
            batch_link: ptr::null_mut(),
            reservation: ReservationNode {
                // Record the current epoch value.
                birth_epoch: self.epoch.load(Ordering::Relaxed),
            },
            batch: BatchNode {
                ref_count: ManuallyDrop::new(AtomicUsize::new(0)),
            },
        }
    }

    // Mark the current thread as active.
    pub fn enter(&self) {
        self.reservations
            .get_or(Default::default)
            .head
            // Acquire: entering a critical section, pointer loads
            // must only occur *after* we mark this thread as active
            .swap(ptr::null_mut(), Ordering::Acquire);
    }

    // Protect an atomic load
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        trace!("protecting pointer");

        if self.epoch_frequency.is_none() {
            return ptr.load(ordering);
        }

        let reservation = self.reservations.get_or(Default::default);

        // Load the last recorded epoch.
        let mut prev_epoch = reservation.epoch.load(Ordering::Relaxed);

        loop {
            let ptr = ptr.load(ordering);

            let current_epoch = self.epoch.load(Ordering::Relaxed);

            if prev_epoch == current_epoch {
                return ptr;
            } else {
                trace!(
                    "updating epoch for from {} to {}",
                    prev_epoch,
                    current_epoch
                );

                // Acquire: entering a critical section, pointer loads
                // must only occur *after* we mark this thread as active
                // in this epoch
                reservation.epoch.swap(current_epoch, Ordering::Acquire);

                prev_epoch = current_epoch;
            }
        }
    }

    pub unsafe fn delayed_retire<T>(
        &self,
        ptr: *mut Linked<T>,
        reclaim: unsafe fn(Link),
    ) -> (bool, &mut Batch) {
        trace!("retiring pointer");

        let batch = &mut *self.batches.get_or(Default::default).get();

        let node = UnsafeCell::raw_get(ptr::addr_of_mut!((*ptr).node));

        (*node).reclaim = reclaim;

        if batch.head.is_null() {
            batch.tail = node;

            // if epoch tracking is disabled, set the minimum epoch of
            // this batch epoch to 0 so that we never skip a thread while
            // retiring (reservation epochs will stay 0 as well).
            if self.epoch_frequency.is_none() {
                (*batch.tail).reservation.min_epoch = 0;
            }

            // implicit `node.batch.ref_count = 0`
        } else {
            // Reuse the birth era of REFS to retain
            // the minimum birth era in the batch.
            //
            // If epoch tracking is disabled this will always be false (0 > 1).
            if (*batch.tail).reservation.min_epoch > (*node).reservation.birth_epoch {
                (*batch.tail).reservation.min_epoch = (*node).reservation.birth_epoch;
            }

            // point to REFS
            (*node).batch_link = batch.tail;

            // link to batch
            (*node).batch.next = batch.head;
        }

        batch.head = node;
        batch.size += 1;

        (batch.size % self.batch_size == 0, batch)
    }

    pub unsafe fn retire_batch(&self) {
        self.retire(&mut *self.batches.get_or(Default::default).get());
    }

    // Attempt to retire nodes in this batch.
    pub unsafe fn retire(&self, batch: &mut Batch) {
        trace!("attempting to retire batch");

        (*batch.tail).batch_link = batch.head;

        let mut last = batch.head;
        for reservation in self.reservations.iter() {
            let head = &reservation.head as *const AtomicPtr<_> as *const AtomicUsize;

            // If this thread is inactive, we can skip it.
            //
            // This has to be an RMW operation, because we
            // _must_ observe any writes in enter.
            if (*head).fetch_add(0, Ordering::Acquire) as *mut Node == Node::INACTIVE {
                continue;
            }

            // If this thread's epoch is behind all the nodes
            // in the batch, we can also skip it.
            //
            // If epoch tracking is disabled this is always false (0 < 0).
            //
            // This has to be an RMW operation, because we
            // _must_ observe any writes in protect.
            if reservation.epoch.fetch_add(0, Ordering::Acquire)
                < (*batch.tail).reservation.min_epoch
            {
                continue;
            }

            if last == batch.tail {
                return;
            }

            (*last).reservation.head = &reservation.head;
            last = (*last).batch.next;
        }

        let mut active = 0;
        let mut curr = batch.head;

        while curr != last {
            let list = (*curr).reservation.head;

            loop {
                let prev = (*list).load(Ordering::Acquire);

                (*curr).reservation.next.store(prev, Ordering::Relaxed);

                // Release: Make the store of `reservation.next` above visible
                // to threads that call `leave`
                if (*list)
                    .compare_exchange_weak(prev, curr, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    active += 1;
                    break;
                }
            }

            curr = (*curr).batch.next;
        }

        // Acquire/Release synchronization is needed here, similar
        // to Arc::drop
        if (*batch.tail)
            .batch
            .ref_count
            .fetch_add(active, Ordering::AcqRel)
            .wrapping_add(active)
            == 0
        {
            Collector::free_list(batch.tail);
        }

        batch.head = ptr::null_mut();
        batch.size = 0;
    }

    // Mark the current thread as inactive.
    pub fn leave(&self) {
        trace!("marking thread as inactive");

        let reservation = self.reservations.get_or(Default::default);

        // Release: Leaving the critical section
        // Acquire: Acquire the store of `reservation.next` in `retire`
        let head = reservation.head.swap(Node::INACTIVE, Ordering::AcqRel);

        if head != Node::INACTIVE {
            unsafe { Collector::traverse(head) }
        }
    }

    // Decrement any reference counts, keeping the thread marked
    // as active.
    pub unsafe fn flush(&self) {
        trace!("flushing guard");

        let reservation = self.reservations.get_or(Default::default);

        // Release: Leaving the critical section
        // Acquire: Entering a new critical section, acquire the store of
        // `reservation.next` in `retire`
        let head = reservation.head.swap(ptr::null_mut(), Ordering::AcqRel);

        if head != Node::INACTIVE {
            Collector::traverse(head)
        }
    }

    // Traverse the reservation list, decrementing the refernce
    // count of each batch.
    unsafe fn traverse(mut list: *mut Node) {
        trace!("decrementing batch reference counts");

        loop {
            let curr = list;
            if curr.is_null() {
                break;
            }

            // This load can be Relaxed because any stores
            // in `retire` are made visible by the Acquire/Release
            // synchronization on `head`
            list = (*curr).reservation.next.load(Ordering::Relaxed);

            let tail = (*curr).batch_link;

            // Acquire/Release synchronization is needed here, similar
            // to Arc::drop
            if (*tail).batch.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                Collector::free_list(tail);
            }
        }
    }

    // Free a reservation list.
    unsafe fn free_list(list: *mut Node) {
        trace!("freeing reservation list");

        let mut list = (*list).batch_link;

        loop {
            let node = list;
            list = (*node).batch.next;
            ((*node).reclaim)(Link { node });

            if list.is_null() {
                break;
            }
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        trace!("dropping collector");

        for batch in self.batches.iter() {
            unsafe {
                let batch = &mut *batch.get();
                if !batch.head.is_null() {
                    (*batch.tail).batch_link = batch.head;
                    // the reference count might not be 0 (null)
                    (*batch.tail).batch.next = ptr::null_mut();
                    Collector::free_list(batch.tail);
                }
            }
        }
    }
}

// Node is attached to every allocated object.
//
// When a node is retired, it becomes one of two types:
// - REFS: the first node in a batch (last in the list), holds the reference counter
// - SLOT: Everyone else
pub struct Node {
    // REFS: first slot node
    // SLOTS: pointer to REFS
    batch_link: *mut Node,
    // Vertical batch list
    batch: BatchNode,
    // Horizontal reservation list
    reservation: ReservationNode,
    // User provided drop glue
    reclaim: unsafe fn(Link),
}

#[repr(C)]
union ReservationNode {
    // Before retiring: The epoch value when this node was created
    birth_epoch: u64,
    // SLOT (after retiring): next node in the reservation list
    next: ManuallyDrop<AtomicPtr<Node>>,
    // SLOT (while retiring): temporary location for an active reservation list
    head: *const AtomicPtr<Node>,
    // REFS: minimum epoch of nodes in a batch
    min_epoch: u64,
}

#[repr(C)]
union BatchNode {
    // REFS: reference counter
    ref_count: ManuallyDrop<AtomicUsize>,
    // SLOT: next node in the batch
    next: *mut Node,
}

impl Node {
    // Represents an inactive thread
    //
    // While null indicates an empty list, INACTIVE
    // indicates the thread is not performing
    // an operation on the datastructure.
    pub const INACTIVE: *mut Node = -1_isize as usize as _;
}

// A per-thread reservation list.
#[repr(C)]
struct Reservation {
    // The head of the list.
    head: AtomicPtr<Node>,
    // The epoch value when the thread associated with this list
    // ast accessed a pointer.
    epoch: AtomicU64,
}

impl Default for Reservation {
    fn default() -> Self {
        Reservation {
            head: AtomicPtr::new(Node::INACTIVE),
            epoch: AtomicU64::new(0),
        }
    }
}

// A batch of nodes waiting to be retired.
pub struct Batch {
    // Head the batch
    head: *mut Node,
    // Tail of the batch (REFS)
    tail: *mut Node,
    // The number of nodes in this batch.
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
