//! This module implements efficient barriers for fast and slow paths.
//! Often you have the ability to sacrifice performance in the slow path for performance in the fast path
//! and this is often something you want to do.
//!
//! Here we have two kinds of barriers, light and heavy ones respectively.
//! They can take advantage of OS provided functionality for process wide memory barriers
//! which means the ability to skip barriers in certain fast paths.
//!
//! This module will conditionally compile an optimized implementation depending on the target OS
//! and will attempt to determine the most efficient setup dynamically at runtime.
//!
//! When no specialized implementation is available we fall back to executing a normal
//! sequentially consistent barrier in both the light and heavy barriers.
//!
//! THIS MODULE IS FULL OF HACKS THAT BLATANTLY ABUSE OS INTERFACES.
//! IT'S A TOTAL MESS AND FOR YOUR OWN MENTAL HEALTH YOU MAY WANT TO AVOID THIS CODE.
//! GIVE A PRAYER TO ANY FUTURE DEVELOPER THAT HAS TO MAINTAIN THIS CODE.
//! IF IT WORKS THEN DON'T TOUCH IT EH?

#[cfg(all(feature = "fast-barrier", target_os = "windows"))]
pub use windows::{light_barrier, strong_barrier};

#[cfg(all(feature = "fast-barrier", target_os = "linux"))]
pub use linux::{light_barrier, strong_barrier};

#[cfg(all(feature = "fast-barrier", target_os = "macos"))]
pub use macos::{light_barrier, strong_barrier};

#[cfg(any(
    not(feature = "fast-barrier"),
    all(
        not(target_os = "linux"),
        not(target_os = "windows"),
        not(target_os = "macos")
    )
))]
pub use fallback::{light_barrier, strong_barrier};

#[cfg(all(feature = "fast-barrier", target_os = "windows"))]
mod windows {
    use core::sync::atomic::{compiler_fence, Ordering};
    use winapi::um::processthreadsapi;

    pub fn strong_barrier() {
        unsafe {
            processthreadsapi::FlushProcessWriteBuffers();
        }
    }

    pub fn light_barrier(ordering: Ordering) {
        compiler_fence(ordering);
    }
}

#[cfg(all(feature = "fast-barrier", target_os = "linux"))]
mod linux {
    use once_cell::sync::Lazy;
    use core::sync::atomic::{compiler_fence, fence, Ordering};

    pub fn strong_barrier() {
        match *STRATEGY {
            Strategy::Membarrier => membarrier::barrier(),
            Strategy::Fallback => fence(Ordering::SeqCst),
        }
    }

    pub fn light_barrier(ordering: Ordering) {
        match *STRATEGY {
            Strategy::Membarrier => compiler_fence(ordering),
            Strategy::Fallback => fence(ordering),
        }
    }

    #[derive(Clone, Copy)]
    enum Strategy {
        Membarrier,
        Fallback,
    }

    static STRATEGY: Lazy<Strategy> = Lazy::new(|| {
        if membarrier::is_supported() {
            Strategy::Membarrier
        } else {
            Strategy::Fallback
        }
    });

    mod membarrier {
        #[repr(i32)]
        #[allow(dead_code, non_camel_case_types, clippy::upper_case_acronyms)]
        enum membarrier_cmd {
            MEMBARRIER_CMD_QUERY = 0,
            MEMBARRIER_CMD_GLOBAL = 1,
            MEMBARRIER_CMD_GLOBAL_EXPEDITED = 1 << 1,
            MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED = 1 << 2,
            MEMBARRIER_CMD_PRIVATE_EXPEDITED = 1 << 3,
            MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED = 1 << 4,
            MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE = 1 << 5,
            MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE = 1 << 6,
        }

        fn sys_membarrier(cmd: membarrier_cmd) -> libc::c_long {
            unsafe {
                #[allow(clippy::unnecessary_cast)]
                libc::syscall(libc::SYS_membarrier, cmd as libc::c_int, 0 as libc::c_int)
            }
        }

        pub fn is_supported() -> bool {
            // Queries which membarrier commands are supported. Checks if private expedited
            // membarrier is supported.
            let ret = sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_QUERY);
            if ret < 0
                || ret & membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED as libc::c_long == 0
                || ret & membarrier_cmd::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED as libc::c_long
                    == 0
            {
                return false;
            }

            // Registers the current process as a user of private expedited membarrier.
            if sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED) < 0 {
                return false;
            }

            true
        }

        macro_rules! fatal_assert {
            ($cond:expr) => {
                if !$cond {
                    #[allow(unused_unsafe)]
                    unsafe {
                        libc::abort();
                    }
                }
            };
        }

        pub fn barrier() {
            fatal_assert!(sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED) >= 0);
        }
    }
}

#[cfg(all(feature = "fast-barrier", target_os = "macos"))]
mod macos {
    use core::ptr::null_mut;
    use core::sync::atomic::{compiler_fence, Ordering};
    use once_cell::sync::Lazy;
    use std::sync::{Mutex, MutexGuard};

    struct Ptr(*mut libc::c_void);

    unsafe impl Send for Ptr {}
    unsafe impl Sync for Ptr {}

    static DUMMY_PAGE: Lazy<Mutex<Ptr>> = Lazy::new(|| Mutex::new(Ptr(null_mut())));

    unsafe fn populate_dummy_page() -> MutexGuard<'static, Ptr> {
        let mut dummy_ptr = DUMMY_PAGE.lock().unwrap();

        if dummy_ptr.0.is_null() {
            unsafe {
                let new_ptr = libc::mmap(
                    null_mut(),
                    1,
                    libc::PROT_READ,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                );

                assert!(new_ptr != libc::MAP_FAILED);
                assert!(libc::mlock(new_ptr, 1) >= 0);

                *dummy_ptr = Ptr(new_ptr);
            }
        }

        dummy_ptr
    }

    pub fn strong_barrier() {
        unsafe {
            let dummy_page = populate_dummy_page();
            assert!(libc::mprotect(dummy_page.0, 1, libc::PROT_READ | libc::PROT_WRITE) >= 0);
            assert!(libc::mprotect(dummy_page.0, 1, libc::PROT_READ) >= 0);
        }
    }

    pub fn light_barrier(ordering: Ordering) {
        compiler_fence(ordering);
    }
}

#[cfg(any(
    not(feature = "fast-barrier"),
    all(
        not(target_os = "linux"),
        not(target_os = "windows"),
        not(target_os = "macos")
    )
))]
mod fallback {
    use core::sync::atomic::{fence, Ordering};

    pub fn strong_barrier() {
        fence(Ordering::SeqCst);
    }

    pub fn light_barrier() {
        fence(Ordering::SeqCst);
    }
}
