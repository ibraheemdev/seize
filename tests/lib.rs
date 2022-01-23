use crystalline::{retire, Crystalline, Protect};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn it_works() {
    const PROTECT_ALL: Protect = Protect(0);

    static mut DROPPED: [bool; 2] = [false; 2];

    struct Foo(usize);

    impl Drop for Foo {
        fn drop(&mut self) {
            unsafe { DROPPED[self.0] = true }
        }
    }

    let crystalline = Arc::new(Crystalline::<3>::new().retire_tick(1));

    let zero = AtomicPtr::new(crystalline.link_boxed(Foo(0)));

    let guard = crystalline.guard();
    let value = guard.protect(|| zero.load(Ordering::Acquire), PROTECT_ALL);
    unsafe { guard.retire(value.as_ptr(), retire::boxed::<Foo>) }

    println!("{}", unsafe { value.deref().0 });

    let barrier = Arc::new(Barrier::new(2));
    let one = Arc::new(AtomicPtr::new(crystalline.link_boxed(Foo(1))));

    let h = std::thread::spawn({
        let foo = one.clone();
        let barrier = barrier.clone();
        let crystalline = crystalline.clone();

        move || {
            let guard = crystalline.guard();
            let value = guard.protect(|| foo.load(Ordering::Acquire), PROTECT_ALL);
            println!("{}", unsafe { value.deref().0 });

            barrier.wait();
            drop(guard);
            barrier.wait();
        }
    });

    barrier.wait(); // wait for thread to access value

    let guard = crystalline.guard();
    let value = guard.protect(|| one.load(Ordering::Acquire), PROTECT_ALL);
    println!("{}", unsafe { value.deref().0 });
    unsafe { guard.retire(value.as_ptr(), retire::boxed::<Foo>) }

    barrier.wait(); // wait for thread to drop guard
    h.join().unwrap();

    drop(guard);

    unsafe { assert_eq!(DROPPED, [true, true]) }
}
