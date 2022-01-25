use crate::protect::{self, Protect};

use crate::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use crate::tls::ThreadLocal;
use crate::utils::{self, CachePadded, U64Padded};
use crate::{Link, Linked};

use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::ptr;

// Fast, lock-free, robust concurrent memory reclamation.
//
// The core algorithm is described [in this paper](https://arxiv.org/pdf/2108.02763.pdf).
pub struct Collector<P: Protect> {
    // The global epoch value.
    epoch: AtomicU64,
    // Per-thread, reservations list slots.
    slots: ThreadLocal<CachePadded<Slots<P>>>,
    // Per-thread batches of retired nodes.
    batches: ThreadLocal<UnsafeCell<CachePadded<Batch>>>,
    // The number of nodes allocated per-thread.
    node_count: ThreadLocal<UnsafeCell<u64>>,
    // The number of node allocations before advancing the global epoch.
    pub(crate) epoch_frequency: u64,
    // The number of nodes in a batch before we free.
    pub(crate) max_batch_size: usize,
}

impl<P: Protect> Collector<P> {
    pub fn with_threads(threads: usize, epoch_frequency: u64, batch_size: usize) -> Self {
        Self {
            epoch: AtomicU64::new(1),
            slots: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            node_count: ThreadLocal::with_capacity(threads),
            epoch_frequency,
            max_batch_size: batch_size,
        }
    }

    // Loads the current epoch value.
    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    // Create a new node, storing the current epoch value.
    pub fn node(&self) -> Node {
        let count = self.node_count.get_or(Default::default).get();

        // SAFETY: node counts are only accessed by the current thread
        unsafe {
            *count += 1;

            if *count % self.epoch_frequency == 0 {
                // advance the global epoch
                self.epoch.fetch_add(1, Ordering::AcqRel);
            }
        }

        Node {
            drop: |_| {},
            batch_link: ptr::null_mut(),
            reservation: ReservationNode {
                birth_epoch: self.epoch(),
            },
            batch: BatchNode {
                ref_count: ManuallyDrop::new(AtomicUsize::new(0)),
            },
        }
    }

    // Protect an atomic load
    pub fn protect<T>(&self, mut load: impl FnMut() -> *mut T, index: usize) -> *mut T {
        let slot = self.slots.get_or(Default::default);

        let mut prev_epoch = slot.epoch[index].load(Ordering::Acquire);

        loop {
            let ptr = load();
            let current_epoch = self.epoch();

            if prev_epoch == current_epoch {
                return ptr;
            } else {
                prev_epoch = self.update_epoch(&slot, current_epoch, index);
            }
        }
    }

    // Clean up the old reservation list and set a new epoch.
    fn update_epoch(&self, slot: &Slots<P>, mut current_epoch: u64, index: usize) -> u64 {
        if !slot.head[index].load(Ordering::Acquire).is_null() {
            let first = slot.head[index].swap(Node::INACTIVE, Ordering::AcqRel);

            if first != Node::INACTIVE {
                unsafe {
                    let batch = self.batches.get_or(Default::default).get();
                    Collector::<P>::clean_up(&mut *batch, first)
                }
            }

            slot.head[index].store(ptr::null_mut(), Ordering::SeqCst);
            current_epoch = self.epoch();
        }

        slot.epoch[index].store(current_epoch, Ordering::SeqCst);
        current_epoch
    }

    // Clean up the old reservation list
    unsafe fn clean_up(batch: &mut Batch, next: *mut Node) {
        if !next.is_null() {
            if batch.age == MAX_AGE {
                Collector::<P>::free_list(batch.list);
                batch.list = ptr::null_mut();
                batch.age = 0;
            }

            batch.age += 1;
            Collector::<P>::traverse(next, batch);
        }
    }

    // Defer deallocation of a value until no threads reference it
    pub unsafe fn retire<T>(&self, ptr: *mut Linked<T>, retire: unsafe fn(Link)) {
        debug_assert!(!ptr.is_null(), "Attempted to retire null pointer");

        let batch = &mut *self.batches.get_or(Default::default).get();
        let node = ptr::addr_of_mut!((*ptr).node);

        (*node).drop = retire;

        if batch.head.is_null() {
            batch.min_epoch = (*node).reservation.birth_epoch;
            batch.tail = node;
        } else {
            if batch.min_epoch > (*node).reservation.birth_epoch {
                batch.min_epoch = (*node).reservation.birth_epoch;
            }

            (*node).batch_link = batch.tail;
            (*node).batch.next = batch.head;
        }

        batch.head = node;
        batch.size += 1;

        if batch.size % self.max_batch_size == 0 {
            (*batch.tail).batch_link = node;
            self.try_retire(batch);
        }
    }

    // Clear all protected slots.
    pub unsafe fn clear_all(&self) {
        let batch = &mut *self.batches.get_or(Default::default).get();

        let mut list: protect::Nodes<P> = Default::default();

        for i in 0..P::SLOTS {
            list[i] =
                self.slots.get_or(Default::default).head[i].swap(Node::INACTIVE, Ordering::AcqRel);
        }

        for i in 0..P::SLOTS {
            if list[i] != Node::INACTIVE {
                Collector::<P>::traverse(list[i], batch)
            }
        }

        Collector::<P>::free_list(batch.list);
        batch.list = ptr::null_mut();
        batch.age = 0;
    }

    // Traverse the reservation list, decrementing the refernce
    // count of each batch.
    unsafe fn traverse(mut list: *mut Node, batch: &mut Batch) {
        loop {
            let curr = list;
            if curr.is_null() {
                break;
            }

            list = (*curr).reservation.next.load(Ordering::Acquire);

            let node = &mut *(*curr).batch_link;
            let x = node.batch.ref_count.fetch_sub(1, Ordering::AcqRel);
            if x == 1 {
                node.reservation.next.store(batch.list, Ordering::Release);
                batch.list = node;
            }
        }
    }

    // Attempt to retire nodes in this batch.
    unsafe fn try_retire(&self, batch: &mut Batch) {
        let mut curr = batch.head;
        let refs = batch.tail;
        let min_epoch = batch.min_epoch;

        let mut last = curr;

        for slot in self.slots.iter() {
            for i in 0..P::SLOTS {
                let first = slot.head[i].load(Ordering::Acquire);
                if first == Node::INACTIVE {
                    continue;
                }

                let epoch = slot.epoch[i].load(Ordering::Acquire);
                if epoch < min_epoch {
                    continue;
                }

                if last == refs {
                    return;
                }

                (*last).reservation.slot = (slot as *const _ as *const AtomicPtr<Node>).add(i);
                last = (*last).batch.next;
            }
        }

        let mut count = 0;

        'walk: while curr != last {
            let slot_first = &*(*curr).reservation.slot;
            let slot_epoch = &*(*curr).reservation.slot.add(P::SLOTS).cast::<AtomicU64>();

            let prev = slot_first.load(Ordering::Acquire);

            loop {
                if prev == Node::INACTIVE {
                    curr = (*curr).batch.next;
                    continue 'walk;
                }

                let epoch = slot_epoch.load(Ordering::Acquire);
                if epoch < min_epoch {
                    curr = (*curr).batch.next;
                    continue 'walk;
                }

                (*curr).reservation.next.store(prev, Ordering::Relaxed);

                if slot_first
                    .compare_exchange_weak(prev, curr, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }

            count += 1;
            curr = (*curr).batch.next;
        }

        if (*refs).batch.ref_count.fetch_add(count, Ordering::AcqRel) + count == 0 {
            (*refs)
                .reservation
                .next
                .store(ptr::null_mut(), Ordering::Release);

            #[cfg(loom)]
            {
                // loom's AtomicUsize is not repr(transparent) over
                // usize, so the 0 value of `batch.ref_count` will not be
                // interpreted as a null `batch.next`.
                (*refs).batch.next = ptr::null_mut();
            }

            Collector::<P>::free_list(&mut *refs);
        }

        batch.head = ptr::null_mut();
        batch.size = 0;
    }

    // Free the reservation list.
    unsafe fn free_list(mut list: *mut Node) {
        while !list.is_null() {
            let mut start = (*list).batch_link;
            list = (*list).reservation.next.load(Ordering::Acquire);

            loop {
                let node = start;
                start = (*node).batch.next;
                ((*node).drop)(Link { node });

                if start.is_null() {
                    break;
                }
            }
        }
    }
}

utils::const_assert!(
    // We need the size of the elements of `reservation.first` to be equal
    // `reservation.epoch`, in order to jump between the two from the pointer
    // stored in `node.reservation.slot`. That way `ReservationNode` stays 64
    // bits.
    std::mem::size_of::<U64Padded<AtomicPtr<Node>>>() == std::mem::size_of::<AtomicU64>()
);

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
    drop: unsafe fn(Link),
}

#[repr(C)]
union ReservationNode {
    // Before retiring: The epoch value when this node was created
    birth_epoch: u64,
    // SLOT (while retiring): next node in the reservation list
    next: ManuallyDrop<AtomicPtr<Node>>,
    // SLOT (after retiring): reservation slot
    slot: *const AtomicPtr<Node>,
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

// Per-slot reservation lists.
#[repr(C)]
struct Slots<P: Protect> {
    // The head node of the resevation list.
    head: protect::AtomicNodes<P>,
    // The epoch value when this slot was last accessed.
    epoch: protect::Epochs<P>,
}

impl<P: Protect> Default for Slots<P> {
    fn default() -> Self {
        Slots {
            head: protect::AtomicNodes::<P>::default(),
            epoch: protect::Epochs::<P>::default(),
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
    // Head of the reservation list
    list: *mut Node,
    // The number of times the epoch was updated.
    age: usize,
    // The minimum epoch across all nodes in this batch.
    min_epoch: u64,
}

// The maximum age of a batch before the reservation list is reset.
const MAX_AGE: usize = 12;

impl Default for Batch {
    fn default() -> Self {
        Batch {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            list: ptr::null_mut(),
            age: 0,
            size: 0,
            min_epoch: 0,
        }
    }
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}
