# Seize

[![Crate](https://img.shields.io/crates/v/seize?style=for-the-badge)](https://crates.io/crates/seize)
[![Github](https://img.shields.io/badge/github-seize-success?style=for-the-badge)](https://github.com/ibraheemdev/seize)
[![Docs](https://img.shields.io/badge/docs.rs-0.2.2-4d76ae?style=for-the-badge)](https://docs.rs/seize)

Fast, efficient, and robust memory reclamation for concurrent data structures.

# Introduction

Concurrent data structures are faced with the problem of deciding when it is
safe to free memory. Although an object might have been logically removed, other
threads that previously loaded it may still be accessing it, and thus it is
not safe to free immediately. Over the years, many algorithms have been devised
to solve this problem. However, most traditional memory reclamation schemes make
the tradeoff between performance, efficiency, and robustness. For example,
[epoch based reclamation] is fast and lightweight but lacks robustness in that a
stalled thread can prevent the reclamation of _all_ retired objects. [Hazard
pointers], another popular scheme, tracks individual pointers, making it efficient
and robust but generally much slower.

Another problem that is often not considered is workload balancing. In most
reclamation schemes, the thread that retires an object is the one that reclaims
it. This leads to unbalanced reclamation in read-dominated workloads;
parallelism is degraded when only a fraction of threads are writing. This is
especially prevalent with the use of M:N threading models as provided by
asynchronous runtimes like [Tokio].

# Details

Seize is based on the [hyaline reclamation scheme], which uses reference counting
to determine when it is safe to free memory. However, reference counters are only
used for objects that have been retired, allowing it to avoid the high overhead
incurred by traditional reference counting schemes where every memory access requires
modifying shared memory. Performance is typically on par with or better than epoch based
schemes, while memory efficiency is similar to hazard pointers. Reclamation is naturally
balanced as the thread with the last reference to an object is the one that frees it.
Epochs are also tracked to protect against stalled threads, making reclamation truly
lock-free.

Seize is compatible with all modern hardware that supports single-word atomic
operations such as FAA and CAS.

# Guide

Seize tries to stay out of your way as much as possible. It works with raw
pointers directly instead of creating safe wrapper types that end up being a
hassle to work with in practice. Below is a step-by-step guide on how to get
started.

### Creating Collectors

Seize avoids the use of global state and encourages creating a designated
_collector_ per data structure. Collectors allow you to allocate, protect, and
retire objects:

```rust,ignore
use seize::Collector;

struct Stack<T> {
    collector: Collector,
    // ...
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self {
            collector: Collector::new(),
        }
    }
}
```

### Allocating Objects

Seize requires storing some metadata about the global epoch for each object that
is allocated. It also needs to reserve a couple words for retirement lists.
Because of this, values in a concurrent data structure must take the form of
`AtomicPtr<seize::Linked<T>>`, as opposed to just `AtomicPtr<T>`.

You can link a value to a collector with the `link` method. `link_boxed` is also
provided as a quick way to link an object to a collector, and allocate it:

```rust
use std::sync::atomic::{AtomicPtr, Ordering};
use std::mem::ManuallyDrop;
use seize::{Collector, Linked};

pub struct Stack<T> {
    head: AtomicPtr<Linked<Node<T>>>, // <===
    collector: Collector,
}

struct Node<T> {
    next: AtomicPtr<Linked<Node<T>>>, // <===
    value: ManuallyDrop<T>,
}

impl<T> Stack<T> {
    pub fn push(&self, value: T) {
        let node = self.collector.link_boxed(Node { // <===
            value: ManuallyDrop::new(value),
            next: AtomicPtr::new(std::ptr::null_mut()),
        });

        // ...
    }
}
```

### Beginning Operations

To begin a concurrent operation, you must mark the thread as _active_ by calling
the `enter` method.

```rust,ignore
impl Stack {
    pub fn push(&self, value: T) {
        // ...

        let guard = self.collector.enter(); // <===

        // ...
    }
}
```

### Protecting Pointers

`enter` returns a guard that allows you to safely load an atomic pointer. Any
_valid_ pointer loaded through a guard is guaranteed to stay valid until the
guard is dropped, or it is retired by the current thread. Importantly, if a
different thread retires an object while we you hold the guard, the collector
knows not to reclaim the object until the guard is dropped.

```rust,ignore
impl Stack {
    pub fn push(&self, value: T) {
        let node = self.collector.link_boxed(Node {
            value: ManuallyDrop::new(value),
            next: AtomicPtr::new(ptr::null_mut()),
        });

        let guard = self.collector.enter();

        loop {
            let head = guard.protect(&self.head, Ordering::Acquire); // <===
            unsafe { (*node).next.store(head, Ordering::Relaxed) }

            if self
                .head
                .compare_exchange(head, node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // drop(guard);
    }
}
```

Note that the lifetime of a guarded pointer is logically tied to that of the
guard -- when the guard is dropped the pointer is invalidated -- but a raw
pointer is returned for convenience.

### Retiring Objects

Objects that have been removed from a data structure can be safely _retired_
through the collector. When no thread holds a reference to it, it's memory will
be reclaimed:

```rust,ignore
impl<T> Stack<T> {
    pub fn pop(&self) -> Option<T> {
        let guard = self.collector.enter(); // <===

        loop {
            let head = guard.protect(&self.head); // <===

            if head.is_null() {
                return None;
            }

            let next = guard.protect(&(*head).next); // <===

            if self
                .head
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    let data = ptr::read(&(*head).data);
                    self.collector.retire(head, |_| {}); // <===
                    return Some(ManuallyDrop::into_inner(data));
                }
            }
        }
    }
}
```

There are a couple important things to note about retiring an object:

#### 1. Retired objects must be logically removed

An object can only be retired if it is _no longer accessible_ to any thread that
comes after. In the above code example this was ensured by swapping out the node
before retiring it. Threads that loaded a value _before_ it was retired are
safe, but threads that come after are not. Note that this means the necessary
memory orderings/fences are required to prevent store-load reordering between
the operation that makes an object unreachable and `retire`.

#### 2. Retired objects cannot be accessed by the current thread

Unlike in schemes like EBR, a guard does not protect objects retired by the
current thread. If no other thread holds a reference to an object it may be
reclaimed _immediately_. This makes the following code unsound:

```rust,ignore
let ptr = guard.protect(&node);
collector.retire(ptr, |_| {});
println!("{}", (*ptr).value); // <===== unsound!
```

Retirement can be delayed until the guard is dropped by calling `reclaim` on
the guard, instead of on the collector directly:

```rust,ignore
let ptr = guard.protect(&node);
guard.retire(ptr, |_| {});
println!("{}", (*ptr).value); // <===== ok!
drop(guard); // <===== ptr is invalidated!
```

#### 3. Custom Reclaimers

You probably noticed that `retire` takes a function as a second parameter. This
function is known as a _reclaimer_, and is run when the collector decides it is
safe to free the retired object. Typically you will pass in a function from the
`reclaim` module. For example, values allocated with `Box` can use
`reclaim::boxed`:

```rust,ignore
use seize::reclaim;

impl<T> Stack<T> {
    pub fn pop(&self) -> Option<T> {
        // ...
        self.collector.retire(head, reclaim::boxed::<Node<T>>); // <===
        // ...
    }
}
```

The type annotation there is important. It is **unsound** to pass a reclaimer of
a different type than the object being retired.

If you need to run custom reclamation code, you can write a custom reclaimer.
Functions passed to `retire` are called with a type-erased `Link`. This is
because retired values are connected to thread-local batches via linked lists,
losing any information. To extract the underlying value from a link, you can
call the `cast` method.

```rust,ignore
collector.reclaim(value, |link: Link| unsafe {
    // SAFETY: the value passed to reclaim was of type
    // `*mut Linked<Value>`
    let ptr: *mut Linked<Value> = link.cast::<Value>();

    // SAFETY: the value was allocated with `link_boxed`
    let value = Box::from_raw(ptr);

    println!("dropping {}", value);

    drop(value);
});
```

[hazard pointers]:
  https://www.cs.otago.ac.nz/cosc440/readings/hazard-pointers.pdf
[hyaline reclamation scheme]: https://arxiv.org/pdf/1905.07903.pdf
[epoch based reclamation]:
  https://www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf
[tokio]: https://github.com/tokio-rs/tokio
