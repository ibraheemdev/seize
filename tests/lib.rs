use crystalline::{Crystalline, Protect};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn it_works() {
    const PROTECT_ONE: Protect = Protect(0);
    const PROTECT_TWO: Protect = Protect(1);

    struct Foo(usize);

    impl Drop for Foo {
        fn drop(&mut self) {
            println!("DROPPED {}!", self.0);
        }
    }

    let crystalline = Arc::new(Crystalline::<3>::new());

    {
        let foo = AtomicPtr::new(crystalline.link_boxed(Foo(98)));

        let guard = crystalline.guard();
        let value = guard.protect(|| foo.load(Ordering::Acquire), PROTECT_ONE);
        unsafe { guard.retire(value) }

        println!("{}", unsafe { value.deref().0 });
    }

    {
        let barrier = Arc::new(Barrier::new(2));
        let foo = Arc::new(AtomicPtr::new(crystalline.link_boxed(Foo(99))));

        std::thread::spawn({
            let foo = foo.clone();
            let barrier = barrier.clone();
            let crystalline = crystalline.clone();

            move || {
                let guard = crystalline.guard();
                let value = guard.protect(|| foo.load(Ordering::Acquire), PROTECT_TWO);
                println!("{}", unsafe { value.deref().0 });

                barrier.wait();
                drop(guard);
                barrier.wait();
            }
        });

        barrier.wait(); // wait for thread to access value

        let guard = crystalline.guard();
        let value = guard.protect(|| foo.load(Ordering::Acquire), PROTECT_TWO);
        println!("{}", unsafe { value.deref().0 });
        unsafe { guard.retire(value) }

        barrier.wait(); // wait for thread to drop guard
    }
}
