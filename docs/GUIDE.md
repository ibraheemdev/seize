A quick-start guide for working with `seize`.

# Introduction

`seize` tries to stay out of your way as much as possible. It works with raw
pointers directly instead of creating safe wrapper types that end up being a
hassle to work with in practice. Below is a step-by-step guide on how to get
started. We'll be writing a stack that implements concurrent `push` and `pop`
operations. The details of how the stack works are not directly relevant, the
guide will instead focus on how `seize` works generally.

# Collectors

`seize` avoids the use of global state and encourages creating a designated
_collector_ per data structure. Collectors allow you to safely read and reclaim
objects. For our concurrent stack, the collector will sit alongside the head
node.

```rust,ignore
use seize::{reclaim, Collector, Linked};
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicPtr, Ordering};

pub struct Stack<T> {
    // The collector for memory reclamation.
    collector: Collector,

    // The head of the stack.
    head: AtomicPtr<Node<T>>,
}

struct Node<T> {
    // The node's value.
    value: ManuallyDrop<T>,

    // The next node in the stack.
    next: *mut Linked<Node<T>>,
}
```

# Performing Operations

Before starting an operation that involves loading objects that may be
reclaimed, you must mark the thread as _active_ by calling the `enter` method.

```rust,ignore
impl Stack {
    pub fn push(&self, value: T) {
        let node = Box::into:raw(Box::new(Node {
            next: std::ptr::null_mut(),
            value: ManuallyDrop::new(value),
        }));

        let guard = self.collector.enter(); // <===

        // ...
    }
}
```

# Protecting Loads

`enter` returns a guard that allows you to safely load atomic pointers. Guards
are the core of safe memory reclamation, letting other threads know that the
current thread may be accessing shared memory.

Using a guard, you cana perform a _protected_ load of an atomic pointer using
the [`Guard::protect`] method. Any valid pointer that is protected is guaranteed
to stay valid until the guard is dropped, or the pointer is retired by the
current thread. Importantly, if another thread retires an object that you
protected, the collector knows not to reclaim the object until your guard is
dropped.

```rust,ignore
impl Stack {
    pub fn push(&self, value: T) {
        // ...

        let guard = self.collector.enter();

        loop {
            let head = guard.protect(&self.head.load, Ordering::Relaxed); // <===
            unsafe { (*node).next = head; }

            if self
                .head
                .compare_exchange(head, node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        drop(guard);
    }
}
```

Notice that the lifetime of a guarded pointer is logically tied to that of the
guard — when the guard is dropped the pointer is invalidated — but we work with
raw pointers for convenience. Data structures that return shared references to
values should ensure that the lifetime of the reference is tied to the lifetime
of a guard.

# Retiring Objects

Objects that have been removed from a data structure can be safely _retired_
through the collector. It will be _reclaimed_, or freed, when no threads holds a
reference to it any longer.

```rust,ignore
impl<T> Stack<T> {
    pub fn pop(&self) -> Option<T> {
        // Mark the thread as active.
        let guard = self.collector.enter();

        loop {
            // Perform a protected load of the head.
            let head = guard.protect(&self.head.load, Ordering::Acquire);

            if head.is_null() {
                return None;
            }

            let next = unsafe { (*head).next };

            // Pop the head from the stack.
            if self
                .head
                .compare_exchange(head, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                unsafe {
                    // Read the value of the previous head.
                    let data = ptr::read(&(*head).value);

                    // Retire the previous head now that it has been popped.
                    self.collector.retire(head, reclaim::boxed); // <===

                    // Return the value.
                    return Some(ManuallyDrop::into_inner(data));
                }
            }
        }
    }
}
```

There are a couple important things to note about retiring an object.

### 1. Retired objects must be logically removed

An object can only be retired if it is _no longer accessible_ to any thread that
comes after. In the above code example this was ensured by swapping out the node
before retiring it. Threads that loaded a value _before_ it was retired are
safe, but threads that come after are not.

Note that concurrent stacks typically suffer from the [ABA problem]. Using
`retire` after popping a node ensures that the node is only freed _after_ all
active threads that could have loaded it exit, avoiding any potential ABA.

### 2. Retired objects cannot be accessed by the current thread

A guard does not protect objects retired by the current thread. If no other
thread holds a reference to an object, it may be reclaimed _immediately_. This
makes the following code unsound.

```rust,ignore
let ptr = guard.protect(&node, Ordering::Acquire);
collector.retire(ptr, reclaim::boxed);

// **Unsound**, the pointer has been retired.
println!("{}", (*ptr).value);
```

Retirement can be delayed until the guard is dropped by calling [`defer_retire`]
on the guard, instead of on the collector directly.

```rust,ignore
let ptr = guard.protect(&node, Ordering::Acquire);
guard.defer_retire(ptr, reclaim::boxed);

// This read is fine.
println!("{}", (*ptr).value);
// However, once the guard is dropped, the pointer is invalidated.
drop(guard);
```

### 3. Custom Reclaimers

You probably noticed that `retire` takes a function as a second parameter. This
function is known as a _reclaimer_, and is run when the collector decides it is
safe to free the retired object. Typically you will pass in a function from the
[`seize::reclaim`] module. For example, values allocated with `Box` can use
[`reclaim::boxed`], as we used in our stack.

```rust,ignore
use seize::reclaim;

impl<T> Stack<T> {
    pub fn pop(&self) -> Option<T> {
        // ...
        self.collector.retire(head, reclaim::boxed);
        // ...
    }
}
```

If you need to run custom reclamation code, you can write a custom reclaimer.

```rust,ignore
collector.retire(value, |value: *mut Node<T>, _collector: &Collector| unsafe {
    // Safety: The value was allocated with `Box::new`.
    let value = Box::from_raw(ptr);
    println!("Dropping {value}");
    drop(value);
});
```

Note that the reclaimer receives a reference to the collector as its second
argument, allowing for recursive reclamation.

[`defer_retire`]:
  https://docs.rs/seize/latest/seize/trait.Guard.html#tymethod.defer_retire
[`Guard::protect`]:
  https://docs.rs/seize/latest/seize/trait.Guard.html#tymethod.protect
[`seize::reclaim`]: https://docs.rs/seize/latest/seize/reclaim/index.html
[`reclaim::boxed`]: https://docs.rs/seize/latest/seize/reclaim/fn.boxed.html
[ABA problem]: https://en.wikipedia.org/wiki/ABA_problem
