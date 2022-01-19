use crystalline::{Atomic, Crystalline, Owned};
use std::sync::{atomic::Ordering, Arc};

#[test]
fn it_works() {
    struct Foo(usize);

    impl Drop for Foo {
        fn drop(&mut self) {
            println!("DROPPED!");
        }
    }

    let crystalline = Arc::new(Crystalline::<3>::new());

    {
        let foo = Atomic::new(Foo(99), &crystalline);
        let guard = crystalline.guard();
        let mut foo_guard = guard.local();
        let value = foo.load(Ordering::Acquire, &mut foo_guard);
        unsafe { guard.retire(value) }
        println!("{}", unsafe { value.deref().0 });
    }

    {
        let foo = Arc::new(Atomic::new(Foo(99), &crystalline));

        std::thread::spawn({
            let foo = foo.clone();
            let c = crystalline.clone();
            move || {
                let guard = c.guard();
                let mut foo_guard = guard.local();
                let value = foo.load(Ordering::Acquire, &mut foo_guard);
                std::thread::sleep(std::time::Duration::from_secs(10));
            }
        });

        std::thread::sleep(std::time::Duration::from_secs(1));
        let guard = crystalline.guard();
        let mut foo_guard = guard.local();
        let value = foo.load(Ordering::Acquire, &mut foo_guard);
        println!("{}", unsafe { value.deref().0 });
        unsafe { guard.retire(value) }
    }
}
