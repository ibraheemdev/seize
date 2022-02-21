pub mod sync {
    pub trait WithMut<T> {
        fn with_mut<R>(&mut self, f: impl FnOnce(&mut T) -> R) -> R;
    }

    #[cfg(loom)]
    pub(crate) mod atomic {
        pub use super::WithMut;

        pub(crate) use loom::sync::atomic::*;
    }

    #[cfg(not(loom))]
    pub(crate) mod atomic {
        pub use super::WithMut;

        impl WithMut<bool> for AtomicBool {
            fn with_mut<R>(&mut self, f: impl FnOnce(&mut bool) -> R) -> R {
                f(self.get_mut())
            }
        }

        impl<T> WithMut<*mut T> for AtomicPtr<T> {
            fn with_mut<R>(&mut self, f: impl FnOnce(&mut *mut T) -> R) -> R {
                f(self.get_mut())
            }
        }

        pub(crate) use std::sync::atomic::*;
    }

    #[cfg(loom)]
    pub use loom::sync::*;

    #[cfg(not(loom))]
    pub use std::sync::*;
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
