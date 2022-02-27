use crate::cfg::trace;
use crate::tls::ThreadLocal;
use crate::utils::{CachePadded, LoadLatest};
use crate::{Link, Linked};

use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::num::NonZeroU64;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use crate::barrier::{light_barrier, strong_barrier};

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
        let birth_epoch = unsafe {
            *count += 1;
            trace!("allocated new value, values: {}", *count);

            match self.epoch_frequency {
                Some(ref freq) if *count % freq.get() == 0 => {
                    // Advance the global epoch
                    //
                    // Like with most counter increments, this can be
                    // relaxed. We don't care when this increment happens,
                    // just that it eventually does.
                    let epoch = self.epoch.fetch_add(1, Ordering::Relaxed);
                    trace!("advancing global epoch to {}", epoch + 1);
                    epoch
                }

                // Record the current epoch value.
                //
                // Note that it's fine if we see older epoch values
                // here, because that just means more threads will
                // be counted as active than might actually be.
                //
                // The only problematic case would be if a node
                // was created with a birth_epoch *later* than the
                // global epoch at the time it was created, as any
                // thread loading it wouldn't record the new epoch.
                // This is impossible as the RMW in `protect` is
                // guaranteed to see the latest epoch epoch and
                // therefore we cannot see a value later than it.
                _ => self.epoch.load(Ordering::Relaxed),
            }
        };

        Node {
            reclaim: |_| {},
            batch_link: ptr::null_mut(),
            reservation: ReservationNode { birth_epoch },
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
            .swap(ptr::null_mut(), Ordering::Relaxed);

        light_barrier();
    }

    // Protect an atomic load
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        trace!("protecting pointer");

        if self.epoch_frequency.is_none() {
            return ptr.load(ordering);
        }

        let reservation = self.reservations.get_or(Default::default);

        // Load the last recorded epoch.
        //
        // Acquire: `ptr` must only be used *after* we
        // check that we are in sync with the global epoch.
        let mut prev_epoch = reservation.epoch.load(Ordering::Acquire);

        loop {
            let ptr = ptr.load(ordering);

            // Acquire: `ptr` must only be used *after* we
            // check that we are in sync with the global epoch.
            //
            // This has to use `load_latest`, because we _must_
            // observe any increments of the global epoch.
            let current_epoch = self.epoch.load_latest(Ordering::Acquire);

            if prev_epoch == current_epoch {
                return ptr;
            } else {
                trace!(
                    "updating epoch for from {} to {}",
                    prev_epoch,
                    current_epoch
                );

                // Acquire: the next load of `ptr` must only occur
                // *after* we mark this thread as active in this epoch.
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

        // SAFETY: Batches are only accessed by a single thread.
        let batch = unsafe { &mut *self.batches.get_or(Default::default).get() };

        let node = UnsafeCell::raw_get(ptr::addr_of_mut!((*ptr).node));

        // SAFETY: `ptr` is guaranteed to be a valid linked pointer.
        //
        // Any other thread with a reference to it will only have an
        // shared reference to the UnsafeCell<Node>, and the caller
        // guarantees that the same pointer is not retired twice, so
        // we can safely write to the node here.
        unsafe { (*node).reclaim = reclaim }

        if batch.head.is_null() {
            batch.tail = node;

            // If epoch tracking is disabled, set the minimum epoch of
            // this batch epoch to 0 so that we never skip a thread while
            // retiring (reservation epochs will stay 0 as well).
            if self.epoch_frequency.is_none() {
                // SAFETY: same as write to `node` above.
                unsafe { (*node).reservation.min_epoch = 0 }
            }

            // Implicit `node.batch.ref_count = 0`
        } else {
            // Reuse the birth era of REFS to retain
            // the minimum birth era in the batch.
            //
            // If epoch tracking is disabled this will always be false (0 > 1).
            //
            // SAFETY: We checked that batch.head != null, therefore batch.tail
            // is a valid pointer.
            unsafe {
                if (*batch.tail).reservation.min_epoch > (*node).reservation.birth_epoch {
                    (*batch.tail).reservation.min_epoch = (*node).reservation.birth_epoch;
                }
            }

            // SAFETY: same as write to `node` above.
            unsafe {
                // The batch link of a slot node points to the tail (REFS).
                (*node).batch_link = batch.tail;

                // Insert this node into the batch.
                (*node).batch.next = batch.head;
            }
        }

        batch.head = node;
        batch.size += 1;

        (batch.size % self.batch_size == 0, batch)
    }

    // # Safety
    //
    // The batch must contain at least one node.
    pub unsafe fn retire_batch(&self) {
        // SAFETY: Guaranteed by caller
        unsafe { self.retire(&mut *self.batches.get_or(Default::default).get()) }
    }

    // Attempt to retire nodes in this batch.
    //
    // # Safety
    //
    // The batch must contain at least one node.
    pub unsafe fn retire(&self, batch: &mut Batch) {
        trace!("attempting to retire batch");
        strong_barrier();

        // SAFETY: Caller guarantees that the batch is not empty,
        // so batch.tail must be valid.
        unsafe { (*batch.tail).batch_link = batch.head }

        let mut last = batch.head;
        for reservation in self.reservations.iter() {
            // If this thread is inactive, we can skip it.
            //
            // This has to use `load_latest` because we _must_
            // observe any stores in `enter`.
            //
            // TODO: this can probably be relaxed
            if reservation.head.load_latest(Ordering::Acquire) == Node::INACTIVE {
                continue;
            }

            // SAFETY: The tail of a batch list is always a REFS node with
            // `min_epoch` initialized.
            let min_epoch = unsafe { (*batch.tail).reservation.min_epoch };

            // If this thread's epoch is behind all the nodes
            // in the batch, we can also skip it.
            //
            // If epoch tracking is disabled this is always false (0 < 0).
            //
            // This has to use `load_latest` because we _must_
            // observe any stores in `protect`.
            //
            // TODO: this can probably be relaxed
            if reservation.epoch.load_latest(Ordering::Acquire) < min_epoch {
                continue;
            }

            if last == batch.tail {
                return;
            }

            // SAFETY: We checked that this is not the last node
            // in the batch list above, so we can never access
            // an invalid link.
            unsafe {
                (*last).reservation.head = &reservation.head;
                last = (*last).batch.next;
            }
        }

        let mut active = 0;
        let mut curr = batch.head;

        while curr != last {
            // SAFETY: All nodes in the batch are valid, and we just
            // initialized `reservation.head` for all nodes until `last`
            // in the loop above.
            let head = unsafe { &*(*curr).reservation.head };

            loop {
                // TODO: this can probably be relaxed
                let prev = head.load(Ordering::Acquire);

                // This can be Relaxed because of the Acquire/Release
                // synchronization on `head`
                //
                // SAFETY: All nodes in the batch are valid.
                unsafe { (*curr).reservation.next.store(prev, Ordering::Relaxed) }

                // Release: Make the store of `reservation.next` above visible
                // to threads that call `leave`
                if head
                    .compare_exchange_weak(prev, curr, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    active += 1;
                    break;
                }
            }

            // SAFETY: We check that we never go past the last
            // reservation node in the loop condition.
            curr = unsafe { (*curr).batch.next };
        }

        // SAFETY: The tail of a batch list is always a REFS node with
        // `ref_count` initialized.
        let ref_count = unsafe { &(*batch.tail).batch.ref_count };

        // Release: If we do free the list here, ensure that any
        // use of the data happens *before* it is freed. If we do
        // not, then release any modifications to the data to
        // the thread that will free the list.
        //
        // Acquire: If we free the list, acquire any modifications
        // to the data released by the threads that decremented
        // the count.
        if ref_count
            .fetch_add(active, Ordering::AcqRel)
            .wrapping_add(active)
            == 0
        {
            // SAFETY: The reference count is 0, meaning that
            // either no threads were active, or they all
            // decremented the count in the time it took
            unsafe { Collector::free_list(batch.tail) }
        }

        batch.head = ptr::null_mut();
        batch.size = 0;
    }

    // Mark the current thread as inactive.
    pub fn leave(&self) {
        trace!("marking thread as inactive");

        let reservation = self.reservations.get_or(Default::default);

        let head = reservation.head.swap(Node::INACTIVE, Ordering::Relaxed);
        light_barrier();

        // Decrement any batch reference counts that were added.
        if head != Node::INACTIVE {
            unsafe { Collector::traverse(head) }
        }
    }

    // Decrement any reference counts, keeping the thread marked
    // as active.
    pub unsafe fn flush(&self) {
        trace!("flushing guard");

        let reservation = self.reservations.get_or(Default::default);

        // Release: Leaving the critical section.
        //
        // Acquire: Entering a new critical section, acquire the store of
        // `reservation.next` in `retire`.
        let head = reservation.head.swap(ptr::null_mut(), Ordering::AcqRel);

        // Decrement any batch reference counts that were added.
        if head != Node::INACTIVE {
            unsafe { Collector::traverse(head) }
        }
    }

    // Traverse the reservation list, decrementing the refernce
    // count of each batch.
    //
    // # Safety
    //
    // `list` must be a valid reservation list.
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
            //
            // SAFETY: `curr` is a valid link in the list.
            list = unsafe { (*curr).reservation.next.load(Ordering::Relaxed) };
            let tail = unsafe { (*curr).batch_link };

            // Release: If we do free the list here, ensure that any
            // use of the data happens *before* it is freed. If we do
            // not, then release any modifications to the data to
            // the thread that will free the list.
            //
            // Acquire: If we free the list, acquire any modifications
            // to the data released by the threads that decremented
            // the count.
            //
            // SAFETY: Reservation lists only comprise of SLOT nodes
            // (not batch.tail), and the batch link of a SLOT node
            // points to the tail of the batch, which is always a
            // REFS node with `ref_count` initialized.
            unsafe {
                if (*tail).batch.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                    // SAFETY: The reference count is 1, meaning that
                    // we have the last refernce to the batch.
                    Collector::free_list(tail)
                }
            }
        }
    }

    // Free a reservation list.
    //
    // # Safety
    //
    // `list` must be the last reference to a REFS node of the batch.
    //
    // The reference count must be zero.
    unsafe fn free_list(list: *mut Node) {
        trace!("freeing reservation list");

        // SAFETY: list is a valid pointer.
        let mut list = unsafe { (*list).batch_link };

        loop {
            let node = list;

            unsafe {
                list = (*node).batch.next;
                ((*node).reclaim)(Link { node });
            }

            // If `node` is the tail node (REFS), then
            // `node.batch.next` will interpret the
            // zero reference count in `node.batch.ref_count`
            // as a null pointer, indicating that we have
            // freed the last node in the list.
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
            // SAFETY: We have &mut self
            let batch = unsafe { &mut *batch.get() };

            if !batch.head.is_null() {
                // SAFETY: batch.head is not null, meaning
                // that batch.tail is valid.
                unsafe {
                    // `free_list` expects the batch link
                    // to point to the head of the list.
                    // Usually this is done in `retire`.
                    (*batch.tail).batch_link = batch.head;

                    // `free_list` expects the tail node's
                    // next link to be null. Usually this is
                    // implied by the reference count field
                    // in the union being zero, but that might
                    // not be the case here, so we have to set
                    // it manually.
                    (*batch.tail).batch.next = ptr::null_mut();
                }

                // SAFETY: We have &mut self.
                unsafe { Collector::free_list(batch.tail) }
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
