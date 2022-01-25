mod collector;
mod drop;
mod protect;
mod raw;
mod tls;
mod utils;

pub use collector::{Collector, Guard, Link, Linked};
pub use drop::*;
pub use protect::{Protect, Slots};

mod sync {
    #[cfg(loom)]
    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{
            fence, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize, Ordering,
        };
    }

    #[cfg(not(loom))]
    pub(crate) mod atomic {
        #[cfg(target_pointer_width = "64")]
        pub use core::sync::atomic::AtomicU64;
        pub(crate) use core::sync::atomic::{
            fence, AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize, Ordering,
        };
    }

    #[cfg(loom)]
    pub use loom::sync::Mutex;

    #[cfg(not(loom))]
    pub use std::sync::Mutex;
}
