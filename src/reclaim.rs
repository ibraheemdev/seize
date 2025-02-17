//! Common memory reclaimers.
//!
//! The functions in this module can be passed to
//! [`retire`](crate::Collector::retire) to free allocated memory or run drop
//! glue. See [the guide](crate#custom-reclaimers) for details about memory
//! reclamation, and writing custom reclaimers.

use std::ptr;

use crate::Collector;

/// Reclaims memory allocated with [`Box`].
///
/// # Safety
///
/// The safety requirements of [`Box::from_raw`] apply.
pub unsafe fn boxed<T>(ptr: *mut T, _collector: &Collector) {
    unsafe { drop(Box::from_raw(ptr)) }
}

/// Reclaims memory by dropping the value in place.
///
/// # Safety
///
/// The safety requirements of [`ptr::drop_in_place`] apply.
pub unsafe fn in_place<T>(ptr: *mut T, _collector: &Collector) {
    unsafe { ptr::drop_in_place::<T>(ptr) }
}
