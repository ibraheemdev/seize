pub mod sync {
    #[cfg(loom)]
    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
    }

    #[cfg(not(loom))]
    pub(crate) mod atomic {
        pub(crate) use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
    }

    #[cfg(loom)]
    pub use loom::sync::Mutex;

    #[cfg(not(loom))]
    pub use std::sync::Mutex;
}

macro_rules! trace {
    ($($tt:tt)*) => {
        #[cfg(feature = "tracing")] {
            tracing::trace!("{:?}: {}", std::thread::current().id(), format_args!($($tt)*))
        }
    }
}

macro_rules! loom {
    ($x:stmt) => {
        #[cfg(loom)]
        {
            $x
        };
    };
}

pub(crate) use {loom, trace};
