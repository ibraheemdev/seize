mod raw;

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicPtr;

pub struct Crystalline<const SLOTS: usize> {
    raw: raw::Crystalline<SLOTS>,
}

impl<const SLOTS: usize> Crystalline<SLOTS> {
    pub fn new() -> Self {
        Self {
            raw: raw::Crystalline::with_threads(num_cpus::get()),
        }
    }

    pub fn alloc<T>(&self, value: T) -> Atomic<T> {
        Atomic {
            ptr: AtomicPtr::new(self.raw.alloc(value)),
        }
    }

    pub fn guard(&self) -> Guard<'_, SLOTS> {
        Guard {
            local_guards: Cell::new(0),
            crystalline: self,
            _not_send: PhantomData,
        }
    }

    pub unsafe fn retire<T>(&self, shared: Shared<'_, T>) {
        self.raw.retire(shared.linked)
    }
}

pub struct Shared<'g, T> {
    linked: *mut raw::Linked<T>,
    guard: PhantomData<&'g T>,
}

impl<T> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Shared {
            linked: self.linked,
            guard: PhantomData,
        }
    }
}

impl<T> Copy for Shared<'_, T> {}

impl<'g, T> Shared<'g, T> {
    pub unsafe fn deref(&self) -> &'g T {
        &(*self.linked).value
    }
}

pub struct Atomic<T> {
    ptr: AtomicPtr<raw::Linked<T>>,
}

impl<T> Atomic<T> {
    pub fn load<'g, const SLOTS: usize>(
        &self,
        guard: &'g mut LocalGuard<'_, SLOTS>,
    ) -> Shared<'g, T> {
        Shared {
            linked: guard.parent.crystalline.raw.load(&self.ptr, guard.index),
            guard: PhantomData,
        }
    }
}

pub struct Guard<'a, const SLOTS: usize> {
    local_guards: Cell<usize>,
    crystalline: &'a Crystalline<SLOTS>,
    _not_send: PhantomData<*mut ()>,
}

impl<const SLOTS: usize> Guard<'_, SLOTS> {
    pub fn local(&self) -> LocalGuard<'_, SLOTS> {
        let index = self.local_guards.get();
        self.local_guards.set(index + 1);
        LocalGuard {
            parent: self,
            index,
        }
    }
}

pub struct LocalGuard<'g, const SLOTS: usize> {
    index: usize,
    parent: &'g Guard<'g, SLOTS>,
}

impl<const SLOTS: usize> Drop for Guard<'_, SLOTS> {
    fn drop(&mut self) {
        unsafe { self.crystalline.raw.clear_all() }
    }
}

#[test]
fn it_works() {
    struct Foo(usize);
    impl Drop for Foo {
        fn drop(&mut self) {
            println!("DROPPED!");
        }
    }

    let crystalline = Crystalline::<3>::new();
    for i in 0..120 {
        let pointer = crystalline.alloc(Foo(i));

        let guard = crystalline.guard();
        let mut guard = guard.local();
        let value = pointer.load(&mut guard);
        println!("{}", unsafe { value.deref().0 });
        unsafe { crystalline.retire(value) }
    }
}
