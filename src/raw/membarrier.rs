//! Memory barriers optimized for RCU, inspired by <https://github.com/jeehoonkang/membarrier-rs>.
//!
//! # Semantics
//!
//! There is a total order over all memory barriers provided by this module:
//! - Light store barriers, created by a pair of [`light_store`] and
//!   [`light_barrier`].
//! - Light load barriers, created by a pair of [`light_barrier`] and
//!   [`light_load`].
//! - Sequentially consistent barriers, or cumulative light barriers.
//! - Heavy barriers, created by [`heavy`].
//!
//! If thread A issues barrier X and thread B issues barrier Y and X occurs
//! before Y in the total order, X is ordered before Y with respect to coherence
//! only if either X or Y is a heavy barrier. In other words, there is no way to
//! establish an ordering between light barriers without the presence of a heavy
//! barrier.
#![allow(dead_code)]

#[cfg(all(target_os = "linux", feature = "fast-barrier", not(miri)))]
pub use linux::*;

#[cfg(all(target_os = "windows", feature = "fast-barrier", not(miri)))]
pub use windows::*;

#[cfg(any(
    not(feature = "fast-barrier"),
    not(any(target_os = "windows", target_os = "linux")),
    miri
))]
pub use default::*;

#[cfg(any(
    not(feature = "fast-barrier"),
    not(any(target_os = "windows", target_os = "linux")),
    miri
))]
mod default {
    use core::sync::atomic::{fence, Ordering};

    pub fn detect() {}

    /// The ordering for a store operation that synchronizes with heavy
    /// barriers.
    ///
    /// Must be followed by a light barrier.
    #[inline]
    pub fn light_store() -> Ordering {
        // Synchronize with `SeqCst` heavy barriers.
        Ordering::SeqCst
    }

    /// Issues a light memory barrier for a preceding store or subsequent load
    /// operation.
    #[inline]
    pub fn light_barrier() {
        // This is a no-op due to strong loads and stores.
    }

    /// The ordering for a load operation that synchronizes with heavy barriers.
    #[inline]
    pub fn light_load() -> Ordering {
        // Participate in the total order established by light and heavy `SeqCst`
        // barriers.
        Ordering::SeqCst
    }

    /// Issues a heavy memory barrier for slow path that synchronizes with light
    /// stores.
    #[inline]
    pub fn heavy() {
        // Synchronize with `SeqCst` light stores.
        fence(Ordering::SeqCst);
    }
}

#[cfg(all(target_os = "linux", feature = "fast-barrier", not(miri)))]
mod linux {
    use std::sync::atomic::{self, AtomicU8, Ordering};

    /// The ordering for a store operation that synchronizes with heavy
    /// barriers.
    ///
    /// Must be followed by a light barrier.
    #[inline]
    pub fn light_store() -> Ordering {
        match STRATEGY.load(Ordering::Relaxed) {
            FALLBACK => Ordering::SeqCst,
            _ => Ordering::Relaxed,
        }
    }

    /// Issues a light memory barrier for a preceding store or subsequent load
    /// operation.
    #[inline]
    pub fn light_barrier() {
        atomic::compiler_fence(atomic::Ordering::SeqCst)
    }

    /// The ordering for a load operation that synchronizes with heavy barriers.
    #[inline]
    pub fn light_load() -> Ordering {
        // There is no difference between `Acquire` and `SeqCst` loads on most
        // platforms, so checking the strategy is not worth it.
        Ordering::SeqCst
    }

    /// Issues a heavy memory barrier for slow path.
    #[inline]
    pub fn heavy() {
        // Issue a private expedited membarrier using the `sys_membarrier()` system
        // call, if supported; otherwise, fall back to `mprotect()`-based
        // process-wide memory barrier.
        match STRATEGY.load(Ordering::Relaxed) {
            MEMBARRIER => membarrier::barrier(),
            MPROTECT => mprotect::barrier(),
            _ => atomic::fence(atomic::Ordering::SeqCst),
        }
    }

    /// Use the `membarrier` system call.
    const MEMBARRIER: u8 = 0;

    /// Use the `mprotect`-based trick.
    const MPROTECT: u8 = 1;

    /// Use `SeqCst` fences.
    const FALLBACK: u8 = 2;

    /// The right strategy to use on the current machine.
    static STRATEGY: AtomicU8 = AtomicU8::new(FALLBACK);

    /// Perform runtime detection for a membarrier strategy.
    pub fn detect() {
        if membarrier::is_supported() {
            STRATEGY.store(MEMBARRIER, Ordering::Relaxed);
        } else if mprotect::is_supported() {
            STRATEGY.store(MPROTECT, Ordering::Relaxed);
        }
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

    mod membarrier {
        /// Commands for the membarrier system call.
        ///
        /// # Caveat
        ///
        /// We're defining it here because, unfortunately, the `libc` crate
        /// currently doesn't expose `membarrier_cmd` for us. You can
        /// find the numbers in the [Linux source code](https://github.com/torvalds/linux/blob/master/include/uapi/linux/membarrier.h).
        ///
        /// This enum should really be `#[repr(libc::c_int)]`, but Rust
        /// currently doesn't allow it.
        #[repr(i32)]
        #[allow(dead_code, non_camel_case_types)]
        enum membarrier_cmd {
            MEMBARRIER_CMD_QUERY = 0,
            MEMBARRIER_CMD_GLOBAL = (1 << 0),
            MEMBARRIER_CMD_GLOBAL_EXPEDITED = (1 << 1),
            MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED = (1 << 2),
            MEMBARRIER_CMD_PRIVATE_EXPEDITED = (1 << 3),
            MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED = (1 << 4),
            MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE = (1 << 5),
            MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE = (1 << 6),
        }

        /// Call the `sys_membarrier` system call.
        #[inline]
        fn sys_membarrier(cmd: membarrier_cmd) -> libc::c_long {
            unsafe { libc::syscall(libc::SYS_membarrier, cmd as libc::c_int, 0 as libc::c_int) }
        }

        /// Returns `true` if the `sys_membarrier` call is available.
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

        /// Executes a heavy `sys_membarrier`-based barrier.
        #[inline]
        pub fn barrier() {
            fatal_assert!(sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED) >= 0);
        }
    }

    mod mprotect {
        use std::cell::UnsafeCell;
        use std::mem::MaybeUninit;
        use std::ptr;
        use std::sync::{atomic, OnceLock};

        struct Barrier {
            lock: UnsafeCell<libc::pthread_mutex_t>,
            page: u64,
            page_size: libc::size_t,
        }

        unsafe impl Sync for Barrier {}

        impl Barrier {
            /// Issues a process-wide barrier by changing access protections of
            /// a single mmap-ed page. This method is not as fast as
            /// the `sys_membarrier()` call, but works very
            /// similarly.
            #[inline]
            fn barrier(&self) {
                let page = self.page as *mut libc::c_void;

                unsafe {
                    // Lock the mutex.
                    fatal_assert!(libc::pthread_mutex_lock(self.lock.get()) == 0);

                    // Set the page access protections to read + write.
                    fatal_assert!(
                        libc::mprotect(page, self.page_size, libc::PROT_READ | libc::PROT_WRITE,)
                            == 0
                    );

                    // Ensure that the page is dirty before we change the protection so that we
                    // prevent the OS from skipping the global TLB flush.
                    let atomic_usize = &*(page as *const atomic::AtomicUsize);
                    atomic_usize.fetch_add(1, atomic::Ordering::SeqCst);

                    // Set the page access protections to none.
                    //
                    // Changing a page protection from read + write to none causes the OS to issue
                    // an interrupt to flush TLBs on all processors. This also results in flushing
                    // the processor buffers.
                    fatal_assert!(libc::mprotect(page, self.page_size, libc::PROT_NONE) == 0);

                    // Unlock the mutex.
                    fatal_assert!(libc::pthread_mutex_unlock(self.lock.get()) == 0);
                }
            }
        }

        /// An alternative solution to `sys_membarrier` that works on older
        /// Linux kernels and x86/x86-64 systems.
        static BARRIER: OnceLock<Barrier> = OnceLock::new();

        /// Returns `true` if the `mprotect`-based trick is supported.
        pub fn is_supported() -> bool {
            cfg!(target_arch = "x86") || cfg!(target_arch = "x86_64")
        }

        /// Executes a heavy `mprotect`-based barrier.
        #[inline]
        pub fn barrier() {
            let barrier = BARRIER.get_or_init(|| {
                unsafe {
                    // Find out the page size on the current system.
                    let page_size = libc::sysconf(libc::_SC_PAGESIZE);
                    fatal_assert!(page_size > 0);
                    let page_size = page_size as libc::size_t;

                    // Create a dummy page.
                    let page = libc::mmap(
                        ptr::null_mut(),
                        page_size,
                        libc::PROT_NONE,
                        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                        -1 as libc::c_int,
                        0 as libc::off_t,
                    );
                    fatal_assert!(page != libc::MAP_FAILED);
                    fatal_assert!(page as libc::size_t % page_size == 0);

                    // Locking the page ensures that it stays in memory during the two mprotect
                    // calls in `Barrier::barrier()`. If the page was unmapped between those calls,
                    // they would not have the expected effect of generating IPI.
                    libc::mlock(page, page_size as libc::size_t);

                    // Initialize the mutex.
                    let lock = UnsafeCell::new(libc::PTHREAD_MUTEX_INITIALIZER);
                    let mut attr = MaybeUninit::<libc::pthread_mutexattr_t>::uninit();
                    fatal_assert!(libc::pthread_mutexattr_init(attr.as_mut_ptr()) == 0);
                    let mut attr = attr.assume_init();
                    fatal_assert!(
                        libc::pthread_mutexattr_settype(&mut attr, libc::PTHREAD_MUTEX_NORMAL) == 0
                    );
                    fatal_assert!(libc::pthread_mutex_init(lock.get(), &attr) == 0);
                    fatal_assert!(libc::pthread_mutexattr_destroy(&mut attr) == 0);

                    let page = page as u64;

                    Barrier {
                        lock,
                        page,
                        page_size,
                    }
                }
            });

            barrier.barrier();
        }
    }
}

#[cfg(all(target_os = "windows", feature = "fast-barrier", not(miri)))]
mod windows {
    use core::sync::atomic::{self, Ordering};
    use windows_sys;

    pub fn detect() {}

    /// The ordering for a store operation that synchronizes with heavy
    /// barriers.
    ///
    /// Must be followed by a light barrier.
    #[inline]
    pub fn light_store() -> Ordering {
        Ordering::Relaxed
    }

    /// Issues a light memory barrier for a preceding store or subsequent load
    /// operation.
    #[inline]
    pub fn light_barrier() {
        atomic::compiler_fence(atomic::Ordering::SeqCst)
    }

    /// The ordering for a load operation that synchronizes with heavy barriers.
    #[inline]
    pub fn light_load() -> Ordering {
        Ordering::Relaxed
    }

    /// Issues a heavy memory barrier for slow path that synchronizes with light
    /// stores.
    #[inline]
    pub fn heavy() {
        // Invoke the `FlushProcessWriteBuffers()` system call.
        unsafe { windows_sys::Win32::System::Threading::FlushProcessWriteBuffers() }
    }
}
