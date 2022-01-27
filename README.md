# Protecting Pointers

To allow for safe memory reclamation, loads of atomic pointers must
be wrapped with the [`protect`] method:

```rust
use seize::Collector;
use std::sync::AtomicPtr;

seize::slot! {
    enum Slot {
        First,
    }
}

let collector: Collector<Slot> = Collector::new();

let value =

{
    let guard = collector.guard();
}
```

When a guard is dropped, *all* pointers loaded with
the guard lose their protection.

Note that unlike in memory reclamation algorithms like
EBR, creating a guard is free. It only exists to help
keep track of the `leave` operation.


When values are retired, they are connected
to thread-local batches via linked lists.
List nodes lose all type information. When
writing custom reclamation code, you have to
extract the underlying value from a type-erased
link. This can be done via the [`cast`](Link::cast)
method.


```rust
# seize::slot! { enum Slot { One } }
# let collector = seize::Collector::<Slot>::new();
use std::sync::atomic::{Ordering, AtomicPtr};

let value = AtomicPtr::new(collector.link_boxed(2_usize));

unsafe {
    let value = value.load(Ordering::Relaxed);
    collector.reclaim(value, |link: Link| unsafe {
        // SAFETY: the value passed to reclaim was of type
        // `*mut Linked<usize>`
        let ptr: *mut Linked<usize> = link.cast::<usize>();

        // SAFETY: the value was allocated with `link_boxed`
        let value = Box::from_raw(ptr);

        println!("dropping {}", value);
        drop(value);
    });
}
```
