use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

/// Pads and aligns a value to the length of a cache line.
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "riscv64",
    ),
    repr(align(32))
)]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "riscv64",
        target_arch = "s390x",
    )),
    repr(align(64))
)]
#[derive(Default)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// Load-dont-modify-write
///
/// `load_rmdw` loads an atomic value through an RMW
/// as opposed to a regular load:
///
/// > Atomic read-modify-write operations shall always read
/// > the last value (in the modification order) written before
/// > the write associated with the read-modify-write operation.
pub trait Rdmw {
    type Output;

    fn load_rdmw(&self, ordering: Ordering) -> Self::Output;
}

impl Rdmw for AtomicUsize {
    type Output = usize;

    fn load_rdmw(&self, ordering: Ordering) -> Self::Output {
        self.fetch_add(0, ordering)
    }
}

impl Rdmw for AtomicU64 {
    type Output = u64;

    fn load_rdmw(&self, ordering: Ordering) -> Self::Output {
        self.fetch_add(0, ordering)
    }
}

impl<T> Rdmw for AtomicPtr<T> {
    type Output = *mut T;

    fn load_rdmw(&self, ordering: Ordering) -> Self::Output {
        unsafe { (*(self as *const _ as *const AtomicUsize)).fetch_add(0, ordering) as *mut _ }
    }
}
