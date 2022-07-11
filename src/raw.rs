use crate::cfg::trace;
use crate::tls::ThreadLocal;
use crate::utils::{CachePadded, Rdmw};
use crate::{Link, Linked};

use std::cell::{Cell, UnsafeCell};
use std::mem::ManuallyDrop;
use std::num::NonZeroU64;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

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

    // Create a new node, storing the current epoch value
    #[allow(clippy::let_and_return)] // cfg::trace
    pub fn node(&self) -> Node {
        let count = self.node_count.get_or(Default::default).get();

        // safety: node counts are only accessed by the current thread
        let birth_epoch = unsafe {
            *count += 1;
            trace!("linked new value, values: {}", *count);

            match self.epoch_frequency {
                Some(ref freq) if *count % freq.get() == 0 => {
                    // advance the global epoch
                    //
                    // like with most counter increments, this can be
                    // relaxed
                    let epoch = self.epoch.fetch_add(1, Ordering::Relaxed);
                    trace!("advancing global epoch to {}", epoch + 1);
                    epoch
                }

                // record the current epoch value
                //
                // note that it's fine if we see older epoch values
                // here, because that just means more threads will
                // be counted as active than might actually be
                //
                // the only problematic case would be if a node
                // was created with a birth_epoch *later* than the
                // global epoch a thread sees when protecting it, as
                // the thread wouldn't record the new epoch.
                // this is impossible as `protect` has acquire
                // semantics and is guaranteed to see this value
                // when the value is stored with release semantics
                //
                // TODO(ibraheem): this requires that the pointer value
                // was stored with release ordering, which is not enforced
                // by the current API
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

    // Mark the current thread as active
    pub fn enter(&self) {
        trace!("marking thread as active");

        let reservation = self.reservations.get_or(Default::default);
        let guards = reservation.guards.get();
        reservation.guards.set(guards + 1);

        if guards == 0 {
            reservation
                .head
                // acquire: acquire the new values of any pointers
                // that were retired while we were inactive
                .swap(ptr::null_mut(), Ordering::Acquire);
        }
    }

    // Mark the current thread as inactive
    pub fn leave(&self) {
        trace!("marking thread as inactive");

        let reservation = self.reservations.get_or(Default::default);
        let guards = reservation.guards.get();
        reservation.guards.set(guards - 1);

        if guards == 1 {
            // release: exit the critical section
            //
            // acquire: acquire any new reservation nodes and
            // the new values of any pointers that were retired
            // for the next time we become active
            let head = reservation.head.swap(Node::INACTIVE, Ordering::AcqRel);

            if head != Node::INACTIVE {
                // decrement any batch reference counts that were added
                unsafe { Collector::traverse(head) }
            }
        }
    }

    // Decrement any reference counts, keeping the thread marked
    // as active
    pub unsafe fn flush(&self) {
        trace!("flushing guard");

        let reservation = self.reservations.get_or(Default::default);
        let guards = reservation.guards.get();

        // we can only take the reservation list if this
        // is the only guard for the current thread, otherwise
        // it may free memory being used by a different guard
        if guards == 1 {
            // release: exit the critical section
            //
            // acquire: acquire any new reservation nodes, and
            // the new values of any pointers that were retired
            // for the next time we become active
            let head = reservation.head.swap(ptr::null_mut(), Ordering::AcqRel);

            if head != Node::INACTIVE {
                // decrement any batch reference counts that were added
                unsafe { Collector::traverse(head) }
            }
        }
    }

    // Protect an atomic load
    #[inline]
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, ordering: Ordering) -> *mut T {
        if self.epoch_frequency.is_none() {
            // epoch tracking is disabled, nothing
            // special needed here
            return ptr.load(ordering);
        }

        let reservation = self.reservations.get_or(Default::default);

        // load the last recorded epoch
        //
        // relaxed: the reservation epoch is only modified
        // by the current thread
        let mut prev_epoch = reservation.epoch.load(Ordering::Relaxed);

        // acquire: acquire the birth epoch of this pointer
        let ordering = at_least_acquire(ordering);

        loop {
            let ptr = ptr.load(ordering);

            // relaxed: we acquired at least the pointer's
            // birth epoch above. we need to record at least that
            // epoch to let other threads know that we can still
            // access the pointer
            let current_epoch = self.epoch.load(Ordering::Relaxed);

            // our epoch is out of date, record the new one
            // and try again
            if prev_epoch != current_epoch {
                trace!(
                    "updating epoch for from {} to {}",
                    prev_epoch,
                    current_epoch
                );

                // acquire: acquire the new values of any pointers
                // that were retired while we were inactive in the
                // new epoch
                reservation.epoch.swap(current_epoch, Ordering::Acquire);
                prev_epoch = current_epoch;
                continue;
            }

            return ptr;
        }
    }

    pub unsafe fn delayed_retire<T>(
        &self,
        ptr: *mut Linked<T>,
        reclaim: unsafe fn(Link),
    ) -> (bool, &mut Batch) {
        // safety: batches are only accessed by the current thread
        let batch = unsafe { &mut *self.batches.get_or(Default::default).get() };

        let node = UnsafeCell::raw_get(ptr::addr_of_mut!((*ptr).node));

        // safety: `ptr` is guaranteed to be a valid pointer
        //
        // any other thread with a reference to the pointer
        // will only have an *shared* reference to the UnsafeCell<Node>,
        // which is allowed to alias. the caller guarantees that the
        // same pointer is not retired twice, so we can safely write
        // to the node here
        unsafe { (*node).reclaim = reclaim }

        if batch.head.is_null() {
            batch.tail = node;
            // implicit `node.batch.ref_count = 0`

            // if epoch tracking is disabled, set the minimum epoch of
            // this batch epoch to 0 so that we never skip a thread
            // while retiring (reservation epochs will stay 0 as well)
            if self.epoch_frequency.is_none() {
                // safety: same as the write to `node` above
                unsafe { (*node).reservation.min_epoch = 0 }
            }
        } else {
            // re-use the birth era of REFS to retain the minimum
            // birth era in the batch. if epoch tracking is disabled
            // this will always be false (0 > 1)
            //
            // safety: we checked that batch.head != null, therefore
            // batch.tail is a valid pointer
            unsafe {
                if (*batch.tail).reservation.min_epoch > (*node).reservation.birth_epoch {
                    (*batch.tail).reservation.min_epoch = (*node).reservation.birth_epoch;
                }
            }

            // safety: same as the write to `node` above
            unsafe {
                // the batch link of a slot node points to the tail (REFS)
                (*node).batch_link = batch.tail;

                // insert this node into the batch
                (*node).batch.next = batch.head;
            }
        }

        batch.head = node;
        batch.size += 1;

        (batch.size % self.batch_size == 0, batch)
    }

    // # Safety
    //
    // The batch must contain at least one node
    pub unsafe fn retire_batch(&self) {
        // safety: guaranteed by caller
        unsafe { self.retire(&mut *self.batches.get_or(Default::default).get()) }
    }

    // Attempt to retire nodes in this batch
    //
    // # Safety
    //
    // The batch must contain at least one node
    pub unsafe fn retire(&self, batch: &mut Batch) {
        trace!("attempting to retire batch");

        // if there are not enough nodes in this batch for
        // active threads, we have to try again later
        //
        // acquire: acquire the entries of any active threads
        //
        // release: ensure any threads that join after see
        // the new values of the pointers that were retired
        if batch.size <= self.reservations.threads.rdmw(Ordering::AcqRel) {
            return;
        }

        // safety: caller guarantees that the batch is not empty,
        // so batch.tail must be valid
        unsafe { (*batch.tail).batch_link = batch.head }

        // safety: the tail of a batch list is always a REFS node with
        // `min_epoch` initialized
        let min_epoch = unsafe { (*batch.tail).reservation.min_epoch };

        let mut last = batch.head;

        // record all current active threads.
        // we need to do this in a separate step to make
        // sure we have enough reservation nodes, as the number
        // of threads can grow dynamically
        for reservation in self.reservations.iter() {
            // if this thread is inactive, we can skip it
            //
            // release: release the new value of pointers in this
            // batch for the thread to acquire the next time
            // it changes state (becomes active/inactive)
            if reservation.head.rdmw(Ordering::Release) == Node::INACTIVE {
                continue;
            }

            // if this thread's epoch is behind the earliest birth epoch
            // in this batch, we can skip it as there is no way it could
            // have accessed any of the pointers in this batch
            //
            // if epoch tracking is disabled this is always false (0 < 0)
            //
            // release: release the new value of pointers in this batch
            // for the thread to acquire the next time it loads a pointer
            // and sees it's epoch is out of date
            if reservation.epoch.rdmw(Ordering::Release) < min_epoch {
                continue;
            }

            // we don't have enough nodes to insert into the reservation
            // lists of all active threads, try again later
            if last == batch.tail {
                return;
            }

            // safety: we checked that this is not the last node
            // in the batch list above, and all nodes in a batch
            // are valid
            unsafe {
                (*last).reservation.head = &reservation.head;
                last = (*last).batch.next;
            }
        }

        let mut active = 0;
        let mut curr = batch.head;

        while curr != last {
            // safety: all nodes in the batch are valid, and we just
            // initialized `reservation.head` for all nodes until `last`
            // in the loop above
            let head = unsafe { &*(*curr).reservation.head };
            let mut prev = head.load(Ordering::Acquire);

            loop {
                // the thread became inactive, skip it.
                //
                // as long as the thread became inactive at some point
                // after we verified it was active, it can no longer access
                // the pointer as it must have acquired the new values
                // that we released through `head`
                if prev == Node::INACTIVE {
                    break;
                }

                // relaxed: acq/rel synchronization is provided by `head`
                unsafe { (*curr).reservation.next.store(prev, Ordering::Relaxed) }

                // release: release the new reservation nodes
                match head.compare_exchange_weak(prev, curr, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        active += 1;
                        break;
                    }
                    // retry
                    Err(found) => {
                        prev = found;
                        continue;
                    }
                }
            }

            curr = unsafe { (*curr).batch.next };
        }

        // safety: the tail of a batch list is always a REFS node with
        // `ref_count` initialized
        let ref_count = unsafe { &(*batch.tail).batch.ref_count };

        // acquire: if we free the list, acquire any
        // modifications to the data released by the threads
        // that decremented the count
        //
        // release: if we don't free the list, release any
        // modifications to the data to the thread that will
        if ref_count
            .fetch_add(active, Ordering::AcqRel)
            .wrapping_add(active)
            == 0
        {
            // safety: The reference count is 0, meaning that
            // either no threads were active, or they have all
            // already decremented the count
            unsafe { Collector::free_list(batch.tail) }
        }

        batch.head = ptr::null_mut();
        batch.size = 0;
    }

    // Traverse the reservation list, decrementing the
    // refernce count of each batch
    //
    // # Safety
    //
    // `list` must be a valid reservation list
    unsafe fn traverse(mut list: *mut Node) {
        trace!("decrementing batch reference counts");

        loop {
            let curr = list;

            if curr.is_null() {
                break;
            }

            // safety: `curr` is a valid link in the list
            //
            // relaxed: any reservation nodes were acquired when
            // we loaded `head`
            list = unsafe { (*curr).reservation.next.load(Ordering::Relaxed) };
            let tail = unsafe { (*curr).batch_link };

            // safety: reservation lists only comprise of SLOT nodes,
            // and the batch link of a SLOT node points to the tail
            // of the batch, which is always a REFS node with `ref_count`
            // initialized
            //
            // acquire: if we free the list, acquire any
            // modifications to the data released by the threads
            // that decremented the count
            //
            // release: if we don't free the list, release any
            // modifications to the data to the thread that will
            unsafe {
                if (*tail).batch.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                    // safety: we have the last reference to the batch
                    Collector::free_list(tail)
                }
            }
        }
    }

    // Free a reservation list
    //
    // # Safety
    //
    // `list` must be the last reference to a REFS node
    // of the batch
    //
    // The reference count must be zero
    unsafe fn free_list(list: *mut Node) {
        trace!("freeing reservation list");

        // safety: `list` is a valid pointer
        let mut list = unsafe { (*list).batch_link };

        loop {
            let node = list;

            unsafe {
                list = (*node).batch.next;
                ((*node).reclaim)(Link { node });
            }

            // if `node` is the tail node (REFS), then
            // `node.batch.next` will interpret the
            // 0 in `node.batch.ref_count` as a null
            // pointer, indicating that we have
            // freed the last node in the list
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
            // safety: We have &mut self
            let batch = unsafe { &mut *batch.get() };

            if !batch.head.is_null() {
                // safety: batch.head is not null, meaning
                // that `batch.tail` is valid
                unsafe {
                    // `free_list` expects the batch link
                    // to point to the head of the list
                    //
                    // usually this is done in `retire`
                    (*batch.tail).batch_link = batch.head;

                    // `free_list` expects the tail node's
                    // link to be null. usually this is
                    // implied by the reference count field
                    // in the union being zero, but that might
                    // not be the case here, so we have to set
                    // it manually
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
// When a node is retired, it becomes one of two types:
//
// - REFS: the first node in a batch (tail of the list),
//   holds the reference count
// - SLOT: everyone else
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
    // an operation on the datastructure
    pub const INACTIVE: *mut Node = -1_isize as usize as _;
}

// A per-thread reservation list
#[repr(C)]
struct Reservation {
    // The head of the list
    head: AtomicPtr<Node>,
    // The epoch value when the thread associated with
    // this list last accessed a pointer
    epoch: AtomicU64,
    // the number of guards created by this thread
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
    // Tail of the batch (REFS)
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

#[inline]
fn at_least_acquire(ordering: Ordering) -> Ordering {
    match ordering {
        Ordering::Acquire => Ordering::Acquire,
        Ordering::SeqCst => Ordering::SeqCst,
        _ => Ordering::Acquire,
    }
}
