//! Common memory reclamation methods.
//!
//! Functions in this module can be passed to [`retire`](crate::Collector::retire)
//! to free allocated memory. See [the usage guide](crate#guide) for details about
//! reclamation in general, and writing custom reclaimers.

use std::ptr;

use crate::Link;

/// Reclaims memory allocated with [`Box`].
///
/// This function calls [`Box::from_raw`] on the linked pointer.
pub unsafe fn boxed<T>(mut link: Link) {
    let _ = Box::from_raw(link.cast::<T>());
}

/// Reclaims memory by dropping the value in place.
///
/// This function calls [`ptr::drop_in_place`] on the linked pointer.
pub unsafe fn in_place<T>(mut link: Link) {
    let _ = ptr::drop_in_place(link.cast::<T>());
}
