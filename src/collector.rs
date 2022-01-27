use crate::{raw, Protect};

use std::marker::PhantomData;

pub struct Collector<P: Protect> {
    raw: raw::Collector<P>,
}

impl<P: Protect> Collector<P> {
    const DEFAULT_EPOCH_TICK: u64 = 110;
    const DEFAULT_RETIRE_TICK: usize = 120;

    pub fn new() -> Self {
        Self {
            raw: raw::Collector::with_threads(
                num_cpus::get(),
                Self::DEFAULT_EPOCH_TICK,
                Self::DEFAULT_RETIRE_TICK,
            ),
        }
    }

    pub fn epoch_frequency(mut self, n: u64) -> Self {
        self.raw.epoch_frequency = n;
        self
    }

    pub fn batch_size(mut self, n: usize) -> Self {
        self.raw.max_batch_size = n;
        self
    }

    pub fn guard(&self) -> Guard<'_, P> {
        Guard {
            collector: self,
            _not_send: PhantomData,
        }
    }

    pub fn link<T>(&self, value: T) -> Linked<T> {
        Linked {
            value,
            node: self.raw.node(),
        }
    }

    pub fn link_boxed<T>(&self, value: T) -> *mut Linked<T> {
        Box::into_raw(Box::new(self.link(value)))
    }
}

pub struct Guard<'a, P: Protect> {
    collector: &'a Collector<P>,
    _not_send: PhantomData<*mut ()>,
}

impl<P: Protect> Guard<'_, P> {
    pub unsafe fn seize<T>(&self, ptr: *mut Linked<T>, drop: unsafe fn(Link)) {
        self.collector.raw.retire(ptr, drop)
    }

    pub fn protect<T>(&self, op: impl FnMut() -> *mut Linked<T>, protection: P) -> *mut Linked<T> {
        self.collector.raw.protect(op, protection.as_index())
    }
}

impl<P: Protect> Drop for Guard<'_, P> {
    fn drop(&mut self) {
        unsafe { self.collector.raw.clear_all() }
    }
}

pub struct Link {
    pub(crate) node: *mut raw::Node,
}

impl Link {
    pub unsafe fn as_ptr<T>(&mut self) -> *mut Linked<T> {
        self.node as *mut _
    }
}

#[repr(C)]
pub struct Linked<T> {
    pub(crate) node: raw::Node,
    value: T,
}

impl<T> Linked<T> {
    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> std::ops::Deref for Linked<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Linked<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
