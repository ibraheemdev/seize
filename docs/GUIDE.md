A quick-start guide for working with `seize`.

# Introduction

Seize tries to stay out of your way as much as possible. It works with raw
pointers directly instead of creating safe wrapper types that end up being a
hassle to work with in practice. Below is a step-by-step guide on how to get
started. We'll be writing a stack that implements concurrent `push` and `pop`
operations. The details of how the stack works are not directly relevant, the
guide will instead focus on how Seize works generally.

# Collectors

Seize avoids the use of global state and encourages creating a designated
_collector_ per data structure. Collectors allow you to allocate, protect, and
retire objects.

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

# Allocating Objects

Seize requires storing some metadata about the global epoch for each object that
is allocated. Because of this, objects in a concurrent data structure that may be
reclaimed must embed the `Link` type or use the `Linked<T>` wrapper provided for
convenience. See [DST Support](#dst-support) for more details.

You can create a `Link` with the `link` method, or allocate and link a value with
the `link_boxed` helper:

```rust
use seize::{reclaim, Collector, Linked};
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicPtr, Ordering};

pub struct Stack<T> {
    head: AtomicPtr<Linked<Node<T>>>, // <===
    collector: Collector,
}

struct Node<T> {
    next: *mut Linked<Node<T>>, // <===
    value: ManuallyDrop<T>,
}

impl<T> Stack<T> {
    pub fn push(&self, value: T) {
        let node = self.collector.link_boxed(Node { // <===
            next: std::ptr::null_mut(),
            value: ManuallyDrop::new(value),
        });

        // ...
    }
}
```

# Starting Operations

Before starting an operation that involves loading objects that may eventually be reclaimed,
you must mark the thread as _active_ by calling the `enter` method.

```rust,ignore
impl Stack {
    pub fn push(&self, value: T) {
        // ...

        let guard = self.collector.enter(); // <===

        // ...
    }
}
```

# Protecting Pointers

`enter` returns a guard that allows you to safely load atomic pointers. Guards are
the core of safe memory reclamation. Any valid pointer loaded through a guard using the
`protect` method is guaranteed to stay valid until the guard is dropped, or is retired
by the current thread. Importantly, if another thread retires an object that you protected,
the collector knows not to reclaim the object until your guard is dropped.

```rust,ignore
impl Stack {
    pub fn push(&self, value: T) {
        // ...

        let guard = self.collector.enter();

        loop {
            let head = guard.protect(&self.head, Ordering::Acquire); // <===
            unsafe { (*node).next = head; }

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
pointer is returned for convenience. Data structures that return shared references
to values should ensure that the lifetime of the reference is tied to the lifetime
of a guard.

# Retiring Objects

Objects that have been removed from a data structure can be safely _retired_
through the collector. It will be _reclaimed_, or freed, when no threads holds
a reference to it:

```rust,ignore
impl<T> Stack<T> {
    pub fn pop(&self) -> Option<T> {
        let guard = self.collector.enter(); // <=== mark the thread as active

        loop {
            let head = guard.protect(&self.head, Ordering::Acquire); // <=== safely load the head

            if head.is_null() {
                return None;
            }

            let next = unsafe { (*head).next };

            if self
                .head
                .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    let data = ptr::read(&(*head).value);
                    self.collector.retire(head, reclaim::boxed::<Linked<Node<T>>>); // <=== retire
                    return Some(ManuallyDrop::into_inner(data));
                }
            }
        }
    }
}
```

There are a couple important things to note about retiring an object:

### Retired objects must be logically removed

An object can only be retired if it is _no longer accessible_ to any thread that
comes after. In the above code example this was ensured by swapping out the node
before retiring it. Threads that loaded a value _before_ it was retired are
safe, but threads that come after are not.

Note that concurrent stacks typically suffer from the [ABA problem]. Using `retire`
after popping a node ensures that the node is only freed _after_ all active threads
that could have loaded it exit, avoiding any potential ABA.

### Retired objects cannot be accessed by the current thread

Unlike in schemes like EBR, a guard does not protect objects retired by the
current thread. If no other thread holds a reference to an object it may be
reclaimed _immediately_. This makes the following code unsound:

```rust,ignore
let ptr = guard.protect(&node, Ordering::Acquire);
collector.retire(ptr, |_| {});
println!("{}", (*ptr).value); // <===== unsound!
```

Retirement can be delayed until the guard is dropped by calling `defer_retire` on
the guard, instead of on the collector directly:

```rust,ignore
let ptr = guard.protect(&node, Ordering::Acquire);
guard.defer_retire(ptr, |_| {});
println!("{}", (*ptr).value); // <===== ok!
drop(guard); // <===== ptr is invalidated
```

### Custom Reclaimers

You probably noticed that `retire` takes a function as a second parameter. This
function is known as a _reclaimer_, and is run when the collector decides it is
safe to free the retired object. Typically you will pass in a function from the
[`seize::reclaim`](https://docs.rs/seize/latest/seize/reclaim/index.html) module.
For example, values allocated with `Box` can use `reclaim::boxed`:

```rust,ignore
use seize::reclaim;

impl<T> Stack<T> {
    pub fn pop(&self) -> Option<T> {
        // ...
        self.collector.retire(head, reclaim::boxed::<Linked<Node<T>>); // <===
        // ...
    }
}
```

The type annotation there is important. It is **unsound** to pass a reclaimer of
a different type than the object being retired.

If you need to run custom reclamation code, you can write a custom reclaimer.
Functions passed to `retire` are called with a type-erased `Link` pointer. This is
because retired values lose any type information during the reclamation process.
To extract the underlying value from a link, you can call the `cast` method:

```rust,ignore
collector.retire(value, |link: *mut Link| unsafe {
    // safety: the value retired was of type *mut Linked<T>
    let ptr: *mut Linked<T> = Link::cast(link);

    // safety: the value was allocated with `link_boxed`
    let value = Box::from_raw(ptr);
    println!("dropping {}", value);
    drop(value);
});
```

## DST Support

Most reclamation use cases can work with `Linked<T>` and avoid working with
links directly. However, advanced use cases such as dynamically sized types
may requie more control over type layout. To support this, seize allows embedding
a `Link` directly in your type. See the [`AsLink`](https://docs.rs/seize/latest/seize/trait.AsLink.html)
trait for more details.

[ABA problem]: https://en.wikipedia.org/wiki/ABA_problem
