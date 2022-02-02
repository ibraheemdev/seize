use crate::cfg::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use crate::cfg::{self, trace};
use crate::tls::ThreadLocal;
use crate::utils::{self, CachePadded, U64Padded};
use crate::{Link, Linked};

use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::ptr;

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
    pub(crate) epoch_frequency: u64,
    // The number of nodes in a batch before we free.
    pub(crate) batch_size: usize,
}

impl Collector {
    pub fn with_threads(threads: usize, epoch_frequency: u64, batch_size: usize) -> Self {
        Self {
            epoch: AtomicU64::new(1),
            reservations: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            node_count: ThreadLocal::with_capacity(threads),
            epoch_frequency,
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

            if *count % self.epoch_frequency == 0 {
                // Advance the global epoch
                //
                // This release store synchronizes with all acquires
                // of the epoch. The relaxed load is fine because we
                // only use it for tracing.
                let _epoch = self.epoch.fetch_add(1, Ordering::Release);
                trace!("advancing global epoch to {}", _epoch + 1);
            }
        }

        Node {
            reclaim: |_| {},
            batch_link: ptr::null_mut(),
            reservation: ReservationNode {
                // All loads of the epoch are Acquire
                birth_epoch: self.epoch.load(Ordering::Acquire),
            },
            batch: BatchNode {
                ref_count: ManuallyDrop::new(AtomicUsize::new(0)),
            },
        }
    }

    // Mark the current thread as active.
    pub fn enter(&self) {
        // TODO: this shouldn't be needed, and is probably a loom
        // bug. putting fences in the correct places (where we
        // have a store-load fence that loom doesn't recognize)
        // does not work
        cfg::loom! { loom::sync::atomic::fence(Ordering::SeqCst) }

        let reservation = self.reservations.get_or(Default::default);
        reservation.head.store(ptr::null_mut(), Ordering::SeqCst);
    }

    // Protect an atomic load
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>) -> *mut T {
        let reservation = self.reservations.get_or(Default::default);

        trace!("protecting pointer");

        let mut prev_epoch = reservation.epoch.load(Ordering::Acquire);

        loop {
            let ptr = ptr.load(Ordering::SeqCst);

            let current_epoch = self.epoch.load(Ordering::Acquire);

            if prev_epoch == current_epoch {
                return ptr;
            } else {
                trace!(
                    "updating epoch for from {} to {}",
                    prev_epoch,
                    current_epoch
                );

                reservation.epoch.store(current_epoch, Ordering::SeqCst);
                prev_epoch = current_epoch;
            }
        }
    }

    // Defer deallocation of a value until no threads reference it
    pub unsafe fn retire<T>(&self, ptr: *mut Linked<T>, reclaim: unsafe fn(Link)) {
        debug_assert!(!ptr.is_null(), "attempted to retire null pointer");

        trace!("retiring pointer");

        let batch = &mut *self.batches.get_or(Default::default).get();
        let node = ptr::addr_of_mut!((*ptr).node);

        (*node).reclaim = reclaim;

        if batch.head.is_null() {
            // REFS node: use the `batch.ref_count = 0`
            // that it was initialized with
            batch.min_epoch = (*node).reservation.birth_epoch;
            batch.tail = node;
        } else {
            if batch.min_epoch > (*node).reservation.birth_epoch {
                batch.min_epoch = (*node).reservation.birth_epoch;
            }

            (*node).batch_link = batch.tail;

            // SLOT node: link it to the batch
            (*node).batch.next = batch.head;
        }

        cfg::loom! {
            // loom's atomic pointer is not repr(transparent) over
            // a regular pointer, so the value of `reservation.birth_epoch`
            // cannot be interpreted as a pointer, even though we never read
            // it (AtomicPtr::store would fail without this under loom)
            (*node).reservation.next = ManuallyDrop::new(AtomicPtr::new(ptr::null_mut()))
        }

        batch.head = node;
        batch.size += 1;

        if batch.size % self.batch_size == 0 {
            (*batch.tail).batch_link = node;
            self.try_retire(batch);
        }
    }

    // Mark the current thread as inactive.
    pub fn leave(&self) {
        trace!("marking thread as inactive");

        let reservation = self.reservations.get_or(Default::default);
        let head = reservation.head.swap(Node::INACTIVE, Ordering::AcqRel);

        if head != Node::INACTIVE {
            unsafe { Collector::traverse(head) }
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

            list = (*curr).reservation.next.load(Ordering::Acquire);

            let refs = (*curr).batch_link;
            if (*refs).batch.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                Collector::free_list(refs);
            }
        }
    }

    // Attempt to retire nodes in this batch.
    unsafe fn try_retire(&self, batch: &mut Batch) {
        trace!("attempting to retire batch");

        cfg::loom! { loom::sync::atomic::fence(Ordering::SeqCst) }

        let mut last = batch.head;
        for reservation in self.reservations.iter() {
            if reservation.head.load(Ordering::Acquire) == Node::INACTIVE {
                continue;
            }

            if reservation.epoch.load(Ordering::Acquire) < batch.min_epoch {
                continue;
            }

            if last == batch.tail {
                return;
            }

            (*last).reservation.list = &**reservation as _;
            last = (*last).batch.next;
        }

        let mut active = 0;
        let mut curr = batch.head;

        while curr != last {
            let reservation = (*curr).reservation.list;

            cfg::loom! {
                // loom's AtomicUsize is not repr(transparent) over
                // usize, so the value of `reservation.slot` cannot be
                // interpreted as a `reservation.next` pointer.
                (*curr).reservation.next = ManuallyDrop::new(AtomicPtr::new(ptr::null_mut()))
            }

            loop {
                let prev = (*reservation).head.load(Ordering::Acquire);
                if prev == Node::INACTIVE {
                    break;
                }

                if (*reservation).epoch.load(Ordering::Acquire) < batch.min_epoch {
                    break;
                }

                (*curr).reservation.next.store(prev, Ordering::Relaxed);

                if (*reservation)
                    .head
                    .compare_exchange_weak(prev, curr, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    active += 1;
                    break;
                }
            }

            curr = (*curr).batch.next;
        }

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

    // Free a reservation list.
    unsafe fn free_list(list: *mut Node) {
        trace!("freeing reservation list");

        cfg::loom! {
            // loom's AtomicUsize is not repr(transparent) over
            // usize, so the 0 value of `batch.ref_count`
            // cannot be interpreted as a null pointer
            (*list).batch.next = ptr::null_mut()
        }

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
    // SLOT (while retiring): next node in the reservation list
    next: ManuallyDrop<AtomicPtr<Node>>,
    // SLOT (after retiring): the reservation list of the thread
    // this node was retired on
    list: *const Reservation,
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
    //
    // Note that operatign systems reserve -` for errors,
    // and it will never represent a valid pointer.
    pub const INACTIVE: *mut Node = -1_isize as usize as _;
}

// A per-thread reservation list.
#[repr(C)]
struct Reservation {
    // The head of the list.
    head: U64Padded<AtomicPtr<Node>>,
    // The epoch value when the thread associated with this list
    // ast accessed a pointer.
    epoch: AtomicU64,
}

utils::const_assert!(
    // We need the size of the elements of `reservation.first` to be equal
    // `reservation.epoch`, in order to jump between the two from the pointer
    // stored in `node.reservation.slot`. That way `ReservationNode` stays 64
    // bits.
    std::mem::size_of::<U64Padded<AtomicPtr<Node>>>() == std::mem::size_of::<AtomicU64>()
);

impl Default for Reservation {
    fn default() -> Self {
        Reservation {
            head: U64Padded::new(AtomicPtr::new(Node::INACTIVE)),
            epoch: AtomicU64::new(0),
        }
    }
}

// A batch of nodes waiting to be retired.
struct Batch {
    // Head the batch
    head: *mut Node,
    // Tail of the batch (REFS)
    tail: *mut Node,
    // The number of nodes in this batch.
    size: usize,
    // The minimum epoch across all nodes in this batch.
    min_epoch: u64,
}

impl Default for Batch {
    fn default() -> Self {
        Batch {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            size: 0,
            min_epoch: 0,
        }
    }
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}
