use crystalline::{retire, Crystalline};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn it_works() {
    crystalline::protection! {
        enum Protect {
            All,
        }
    }

    static mut DROPPED: [bool; 2] = [false; 2];

    struct Foo(usize);

    impl Drop for Foo {
        fn drop(&mut self) {
            unsafe { DROPPED[self.0] = true }
        }
    }

    let crystalline = Arc::new(Crystalline::<Protect>::new().retire_frequency(1));

    let zero = AtomicPtr::new(crystalline.link_boxed(Foo(0)));

    let guard = crystalline.guard();
    let value = guard.protect(|| zero.load(Ordering::Acquire), Protect::All);
    unsafe { guard.retire(value, retire::boxed::<Foo>) }

    println!("{}", unsafe { (*value).0 });

    let barrier = Arc::new(Barrier::new(2));
    let one = Arc::new(AtomicPtr::new(crystalline.link_boxed(Foo(1))));

    let h = std::thread::spawn({
        let foo = one.clone();
        let barrier = barrier.clone();
        let crystalline = crystalline.clone();

        move || {
            let guard = crystalline.guard();
            let value = guard.protect(|| foo.load(Ordering::Acquire), Protect::All);
            println!("{}", unsafe { (*value).0 });

            barrier.wait();
            drop(guard);
            barrier.wait();
        }
    });

    barrier.wait(); // wait for thread to access value

    let guard = crystalline.guard();
    let value = guard.protect(|| one.load(Ordering::Acquire), Protect::All);
    println!("{}", unsafe { (*value).0 });
    unsafe { guard.retire(value, retire::boxed::<Foo>) }

    barrier.wait(); // wait for thread to drop guard
    h.join().unwrap();

    drop(guard);

    unsafe { assert_eq!(DROPPED, [true, true]) }
}
