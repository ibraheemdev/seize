mod raw;

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, Ordering};

pub struct Crystalline<const SLOTS: usize> {
    raw: raw::Crystalline<SLOTS>,
}

impl<const SLOTS: usize> Crystalline<SLOTS> {
    pub fn new() -> Self {
        Self {
            raw: raw::Crystalline::with_threads(1),
        }
    }

    pub fn guard(&self) -> Guard<'_, SLOTS> {
        Guard {
            local_guards: Cell::new(0),
            crystalline: self,
            _not_send: PhantomData,
        }
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

pub struct Owned<T> {
    linked: *mut raw::Linked<T>,
}

impl<T> Owned<T> {
    pub fn new<const SLOTS: usize>(value: T, crystalline: &Crystalline<SLOTS>) -> Owned<T> {
        Owned {
            linked: Box::into_raw(crystalline.raw.alloc(value)),
        }
    }
}

pub struct Atomic<T> {
    linked: AtomicPtr<raw::Linked<T>>,
}

impl<T> Atomic<T> {
    pub fn new<const SLOTS: usize>(value: T, crystalline: &Crystalline<SLOTS>) -> Atomic<T> {
        Atomic {
            linked: AtomicPtr::new(Box::into_raw(crystalline.raw.alloc(value))),
        }
    }

    pub fn null() -> Atomic<T> {
        Self {
            linked: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    pub fn load<'g, const SLOTS: usize>(
        &self,
        ordering: Ordering,
        guard: &'g mut LocalGuard<'_, SLOTS>,
    ) -> Shared<'g, T> {
        let linked = guard
            .parent
            .crystalline
            .raw
            .protect(&self.linked, ordering, guard.index);

        Shared {
            linked,
            guard: PhantomData,
        }
    }

    pub fn store<P>(&self, new: P, ordering: Ordering)
    where
        P: Pointer<T>,
    {
        self.linked.store(new.as_linked_ptr(), ordering);
    }

    #[allow(unused_variables)]
    pub fn swap<'g, P, const SLOTS: usize>(
        &self,
        new: P,
        ordering: Ordering,
        guard: &'g mut LocalGuard<'_, SLOTS>,
    ) -> Shared<'g, T>
    where
        P: Pointer<T>,
    {
        let linked = self.linked.swap(new.as_linked_ptr(), ordering);

        Shared {
            linked,
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
        // let index = self.local_guards.get();
        // self.local_guards.set(index + 1);
        // TODO
        LocalGuard {
            parent: self,
            index: 0,
        }
    }

    pub unsafe fn retire<T>(&self, shared: Shared<'_, T>) {
        self.crystalline.raw.retire(shared.linked)
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

pub trait Pointer<T>: sealed::AsLinkedPointer<T> {}
impl<T, U> Pointer<U> for T where T: sealed::AsLinkedPointer<U> {}

mod sealed {
    use super::*;

    pub trait AsLinkedPointer<T> {
        fn as_linked_ptr(&self) -> *mut raw::Linked<T>;
    }

    impl<T> AsLinkedPointer<T> for Owned<T> {
        fn as_linked_ptr(&self) -> *mut raw::Linked<T> {
            self.linked
        }
    }

    impl<T> AsLinkedPointer<T> for Shared<'_, T> {
        fn as_linked_ptr(&self) -> *mut raw::Linked<T> {
            self.linked
        }
    }
}
