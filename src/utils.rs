use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::slice;
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

unsafe impl<T: Zeroable> Zeroable for CachePadded<T> {}
impl<T: NoDropGlue> NoDropGlue for CachePadded<T> {}

/// Read-don't-modify-write
///
/// `rmdw` loads an atomic value through an RMW
/// as opposed to a regular load:
///
/// > Atomic read-modify-write operations shall always read
/// > the last value (in the modification order) written before
/// > the write associated with the read-modify-write operation.
///
/// This also allows loads with `Release` semantics.
pub trait Rdmw {
    type Output;

    fn rdmw(&self, ordering: Ordering) -> Self::Output;
}

impl Rdmw for AtomicUsize {
    type Output = usize;

    fn rdmw(&self, ordering: Ordering) -> Self::Output {
        self.fetch_add(0, ordering)
    }
}

impl Rdmw for AtomicU64 {
    type Output = u64;

    fn rdmw(&self, ordering: Ordering) -> Self::Output {
        self.fetch_add(0, ordering)
    }
}

impl<T> Rdmw for AtomicPtr<T> {
    type Output = *mut T;

    fn rdmw(&self, ordering: Ordering) -> Self::Output {
        // the int2ptr cast here is sketchy, but we never
        // actually dereference the pointer, it's only used
        // for comparisons.
        unsafe { (*(self as *const _ as *const AtomicUsize)).fetch_add(0, ordering) as *mut _ }
    }
}

// # Safety
//
// Same as mem::zeroed::<T>().
#[allow(clippy::missing_safety_doc)] // false positive?
pub unsafe trait Zeroable {}

// A type that requires no drop glue
pub trait NoDropGlue {}

unsafe impl<T: Zeroable> Zeroable for UnsafeCell<T> {}
unsafe impl Zeroable for u64 {}

impl<T: NoDropGlue> NoDropGlue for UnsafeCell<T> {}
impl NoDropGlue for u64 {}

pub fn alloc_zeroed_slice<T: Zeroable>(len: usize) -> Box<[T]> {
    let layout = Layout::array::<T>(len).unwrap();
    let ptr = unsafe { alloc::alloc_zeroed(layout) } as *mut T;

    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }

    unsafe { Box::from_raw(slice::from_raw_parts_mut(ptr, len) as _) }
}
