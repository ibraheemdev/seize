//! Common memory reclaimers.
//!
//! Functions in this module can be passed to [`retire`](crate::Collector::retire)
//! to free allocated memory or run drop glue. See [the guide](crate#3-retiring-objects)
//! for details about memory reclamation and writing custom reclaimers.

use std::ptr;

use crate::Link;

/// A reclaimer decides what function is run to reclaim an object.
///
/// This trait is implemented for types in the [`reclaim`](crate::reclaim)
/// module, as well as custom reclaimer functions. See
/// [`Collector::retire`](crate::Collector::retire) for details.
pub trait Reclaim<T> {
    /// Returns the reclaim function.
    fn reclaimer(self) -> unsafe fn(Link);
}

impl<T> Reclaim<T> for unsafe fn(Link) {
    fn reclaimer(self) -> unsafe fn(Link) {
        self
    }
}

/// Reclaims memory allocated with [`Box`].
///
/// This function calls [`Box::from_raw`] on the linked pointer.
pub struct Boxed;

impl<T> Reclaim<T> for Boxed {
    fn reclaimer(self) -> unsafe fn(Link) {
        |mut link| unsafe {
            let _ = Box::from_raw(link.cast::<T>());
        }
    }
}

/// Reclaims memory by dropping the value in place.
///
/// This implementation calls [`ptr::drop_in_place`] on the linked pointer.
pub struct InPlace;

impl<T> Reclaim<T> for InPlace {
    fn reclaimer(self) -> unsafe fn(Link) {
        |mut link| unsafe {
            ptr::drop_in_place(link.cast::<T>());
        }
    }
}
