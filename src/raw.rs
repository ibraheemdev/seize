use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use thread_local::ThreadLocal;

pub struct Crystalline<const SLOTS: usize> {
    epoch: AtomicU64,
    slots: ThreadLocal<Slots<SLOTS>>,
    batches: ThreadLocal<UnsafeCell<Batch>>,
    allocs: ThreadLocal<AtomicU64>,
}

const EPOCH_TICK: u64 = 110;
const RETIRE_TICK: usize = 1;
const MAX_NODES: usize = 12;

impl<const SLOTS: usize> Crystalline<SLOTS> {
    pub fn with_threads(threads: usize) -> Self {
        if SLOTS > u8::MAX as _ {
            panic!("slots cannot be greater than {}", u8::MAX);
        }

        Self {
            epoch: AtomicU64::new(1),
            slots: ThreadLocal::with_capacity(threads),
            batches: ThreadLocal::with_capacity(threads),
            allocs: ThreadLocal::with_capacity(threads),
        }
    }

    pub fn alloc<T>(&self, value: T) -> Box<Linked<T>> {
        let count = self.allocs.get_or_default().fetch_add(1, Ordering::Relaxed);

        if (count + 1) % EPOCH_TICK == 0 {
            self.epoch.fetch_add(1, Ordering::AcqRel);
        }

        unsafe fn drop_node<T>(node: *mut Node) {
            let _ = Box::from_raw(Linked::<T>::from_node(node));
        }

        let data = Linked {
            value,
            node: Node {
                drop: drop_node::<T>,
                batch_link: ptr::null_mut(),
                birth_epoch: self.epoch(),
                reservation: ReservationNode {
                    next: ManuallyDrop::new(AtomicPtr::default()),
                },
                batch: BatchNode {
                    ref_count: ManuallyDrop::new(AtomicUsize::default()),
                },
            },
        };

        Box::new(data)
    }

    pub fn protect<T>(&self, ptr: &AtomicPtr<T>, ordering: Ordering, index: usize) -> *mut T {
        let slot = self.slots.get_or_default();

        let mut prev_epoch = slot.epoch[index].load(Ordering::Acquire);

        loop {
            let ptr = ptr.load(ordering);
            let current_epoch = self.epoch();

            if prev_epoch == current_epoch {
                return ptr;
            } else {
                prev_epoch = self.update_epoch(&slot, current_epoch, index);
            }
        }
    }

    pub unsafe fn retire<T>(&self, ptr: *mut Linked<T>) {
        debug_assert!(!ptr.is_null(), "Attempted to retire null pointer");

        let batch = &mut *self.batches.get_or_default().get();
        let node = ptr::addr_of_mut!((*ptr).node);

        if batch.first.is_null() {
            batch.min_epoch = (*node).birth_epoch;
            batch.last = node;
        } else {
            if batch.min_epoch > (*node).birth_epoch {
                batch.min_epoch = (*node).birth_epoch;
            }

            (*node).batch_link = batch.last;
        }

        (*node).batch.next = batch.first;
        batch.first = node;
        batch.counter += 1;

        if batch.counter % RETIRE_TICK == 0 {
            (*batch.last).batch_link = node;
            self.try_retire(batch);
        }
    }

    pub unsafe fn clear_all(&self) {
        let batch = &mut *self.batches.get_or_default().get();

        let mut first: [*mut Node; SLOTS] = [ptr::null_mut(); SLOTS];

        for i in 0..SLOTS {
            first[i] = self.slots.get_or_default().first[i].swap(Node::INACTIVE, Ordering::AcqRel);
        }

        for i in 0..SLOTS {
            if first[i] != Node::INACTIVE {
                Crystalline::<SLOTS>::traverse(batch, first[i])
            }
        }

        Crystalline::<SLOTS>::free_batch(batch.nodes);
        batch.nodes = ptr::null_mut();
        batch.node_count = 0;
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    unsafe fn traverse(batch: &mut Batch, mut next: *mut Node) {
        loop {
            let curr = next;
            if curr.is_null() {
                break;
            }

            next = (*curr).reservation.next.load(Ordering::Acquire);
            let node = &mut *(*curr).batch_link;
            if node.batch.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
                node.reservation.next.store(batch.nodes, Ordering::Release);
                batch.nodes = node;
            }
        }
    }

    unsafe fn traverse_cache(batch: &mut Batch, next: *mut Node) {
        if !next.is_null() {
            if batch.node_count == MAX_NODES {
                Crystalline::<SLOTS>::free_batch(batch.nodes);
                batch.nodes = ptr::null_mut();
                batch.node_count = 0;
            }
            batch.node_count += 1;
            Crystalline::<SLOTS>::traverse(batch, next);
        }
    }

    unsafe fn free_batch(mut nodes: *mut Node) {
        while !nodes.is_null() {
            let mut start = (*nodes).batch_link;
            nodes = (*nodes).reservation.next.load(Ordering::Acquire);

            loop {
                let node = start;
                start = (*node).batch.next;
                ((*node).drop)(node);

                if start.is_null() {
                    break;
                }
            }
        }
    }

    unsafe fn try_retire(&self, batch: &mut Batch) {
        let mut curr = batch.first;
        let refs = batch.last;
        let min_epoch = batch.min_epoch;

        let mut last = curr;

        for slot in self.slots.iter() {
            for i in 0..SLOTS {
                let first = slot.first[i].load(Ordering::Acquire);
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

                (*last).reservation.slot = slot.first.as_ptr().add(i);
                last = (*last).batch.next;
            }
        }

        let mut adjs = 0;

        'walk: while curr != last {
            let slot_first = &*(*curr).reservation.slot;
            let slot_epoch = &*(*curr).reservation.slot.add(SLOTS).cast::<AtomicU64>();

            let prev = slot_first.load(Ordering::Acquire);

            loop {
                if prev == Node::INACTIVE {
                    continue 'walk;
                }

                let epoch = slot_epoch.load(Ordering::Acquire);
                if epoch < min_epoch {
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

            adjs += 1;
            curr = (*curr).batch.next;
        }

        if (*refs).batch.ref_count.fetch_add(adjs, Ordering::AcqRel) == 0 && adjs == 0 {
            (*refs)
                .reservation
                .next
                .store(ptr::null_mut(), Ordering::SeqCst);
            Crystalline::<SLOTS>::free_batch(&mut *refs);
        }

        batch.first = ptr::null_mut();
        batch.counter = 0;
    }

    fn update_epoch(&self, slot: &Slots<SLOTS>, mut current_epoch: u64, index: usize) -> u64 {
        if !slot.first[index].load(Ordering::Acquire).is_null() {
            let first = slot.first[index].swap(Node::INACTIVE, Ordering::AcqRel);
            if first != Node::INACTIVE {
                unsafe {
                    let batch = self.batches.get_or_default().get();
                    Crystalline::<SLOTS>::traverse_cache(&mut *batch, first)
                }
            }

            slot.first[index].store(ptr::null_mut(), Ordering::SeqCst);
            current_epoch = self.epoch();
        }

        slot.epoch[index].store(current_epoch, Ordering::SeqCst);
        current_epoch
    }
}

struct Batch {
    first: *mut Node,
    last: *mut Node,
    nodes: *mut Node,
    node_count: usize,
    counter: usize,
    min_epoch: u64,
}

impl Default for Batch {
    fn default() -> Self {
        Batch {
            first: ptr::null_mut(),
            last: ptr::null_mut(),
            nodes: ptr::null_mut(),
            node_count: 0,
            counter: 0,
            min_epoch: 0,
        }
    }
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}

#[repr(C)]
struct Slots<const SLOTS: usize> {
    first: [AtomicPtr<Node>; SLOTS],
    epoch: [AtomicU64; SLOTS],
}

impl<const SLOTS: usize> Default for Slots<SLOTS> {
    fn default() -> Self {
        const ZERO: AtomicU64 = AtomicU64::new(0);
        const INACTIVE: AtomicPtr<Node> = AtomicPtr::new(Node::INACTIVE);

        Slots {
            first: [INACTIVE; SLOTS],
            epoch: [ZERO; SLOTS],
        }
    }
}

pub struct Node {
    batch: BatchNode,
    reservation: ReservationNode,
    drop: unsafe fn(*mut Node),
    batch_link: *mut Node,
    birth_epoch: u64,
}

impl Node {
    const INACTIVE: *mut Node = -1_isize as usize as _;
}

union ReservationNode {
    next: ManuallyDrop<AtomicPtr<Node>>,
    slot: *const AtomicPtr<Node>,
}

union BatchNode {
    ref_count: ManuallyDrop<AtomicUsize>,
    next: *mut Node,
}

#[repr(C)]
pub struct Linked<T> {
    node: Node, // Invariant: Info must come first
    pub(crate) value: T,
}

impl<T> Linked<T> {
    unsafe fn from_node(node: *mut Node) -> *mut Linked<T> {
        node as _
    }
}
