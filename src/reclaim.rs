//! Common memory reclaimers.
//!
//! Functions in this module can be passed to
//! [`retire`](crate::Collector::retire) to free allocated memory or run drop
//! glue. See [the guide](crate#custom-reclaimers) for details about memory
//! reclamation, and writing custom reclaimers.

use std::ptr;

/// Reclaims memory allocated with [`Box`].
///
/// This function calls [`Box::from_raw`] on the linked pointer.
///
/// # Safety
///
/// Ensure that the correct type annotations are used when
/// passing this function to [`retire`](crate::Collector::retire).
/// The value retired must have been of type `T` to be retired through
/// `boxed::<T>`.
pub unsafe fn boxed<T>(link: *mut T) {
    unsafe {
        let _: Box<T> = Box::from_raw(link.cast::<T>());
    }
}

/// Reclaims memory by dropping the value in place.
///
/// This function calls [`ptr::drop_in_place`] on the linked pointer.
///
/// # Safety
///
/// Ensure that the correct type annotations are used when
/// passing this function to [`retire`](crate::Collector::retire).
/// The value retired must have been of type `T` to be retired through
/// `in_place::<T>`.
pub unsafe fn in_place<T>(link: *mut T) {
    unsafe {
        ptr::drop_in_place::<T>(link.cast::<T>());
    }
}
