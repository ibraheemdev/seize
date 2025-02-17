// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

mod thread_id;

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{self, AtomicBool, AtomicPtr, Ordering};
use std::{mem, ptr};

pub use thread_id::Thread;

/// Per-object thread local storage.
pub struct ThreadLocal<T> {
    /// Buckets with increasing power-of-two sizes.
    buckets: [AtomicPtr<Entry<T>>; thread_id::BUCKETS],
}

/// An entry in a `ThreadLocal`.
struct Entry<T> {
    /// A flag for initialization.
    present: AtomicBool,

    /// The value for this entry.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// Safety:
///
/// - We expose mutable references to values when the `ThreadLocal` is dropped,
///   hence `T: Send`.
/// - However, it is impossible to obtain shared references to `T`s except by
///   sharing the `ThreadLocal`, so `T: Sync` is not required.
unsafe impl<T: Send> Send for ThreadLocal<T> {}

/// Safety:
///
/// - Values can be inserted through a shared reference and thus dropped
///   on another thread than they were created on, hence `T: Send`.
/// - However, there is no way to access a `T` inserted by another thread
///   except through iteration, which is unsafe, so `T: Sync` is not required.
unsafe impl<T: Send> Sync for ThreadLocal<T> {}

impl<T> ThreadLocal<T> {
    /// Create a `ThreadLocal` container with the given initial capacity.
    pub fn with_capacity(capacity: usize) -> ThreadLocal<T> {
        let init = match capacity {
            0 => 0,
            // Initialize enough buckets for `capacity` elements.
            n => Thread::new(n).bucket,
        };

        let mut buckets = [ptr::null_mut(); thread_id::BUCKETS];

        // Initialize the initial buckets.
        for (i, bucket) in buckets[..=init].iter_mut().enumerate() {
            let bucket_size = Thread::bucket_capacity(i);
            *bucket = allocate_bucket::<T>(bucket_size);
        }

        ThreadLocal {
            // Safety: `AtomicPtr<T>` has the same representation as `*mut T`.
            buckets: unsafe { mem::transmute(buckets) },
        }
    }

    /// Load the slot for the given `thread`, initializing it with a default
    /// value if necessary.
    ///
    /// # Safety
    ///
    /// The current thread must have unique access to the slot for the provided
    /// `thread`.
    #[inline]
    pub unsafe fn load(&self, thread: Thread) -> &T
    where
        T: Default,
    {
        // Safety: Guaranteed by caller.
        unsafe { self.load_or(T::default, thread) }
    }

    /// Load the entry for the given `thread`, initializing it using the
    /// provided function if necessary.
    ///
    /// # Safety
    ///
    /// The current thread must have unique access to the slot for the given
    /// `thread`.
    #[inline]
    pub unsafe fn load_or(&self, create: impl Fn() -> T, thread: Thread) -> &T {
        // Safety: `thread.bucket` is always in bounds.
        let bucket = unsafe { self.buckets.get_unchecked(thread.bucket) };
        let mut bucket_ptr = bucket.load(Ordering::Acquire);

        if bucket_ptr.is_null() {
            bucket_ptr = self.initialize(bucket, thread);
        }

        // Safety: `thread.entry` is always in bounds, and we ensured the bucket was
        // initialized above.
        let entry = unsafe { &*bucket_ptr.add(thread.entry) };

        // Relaxed: Only the current thread can set the value.
        if !entry.present.load(Ordering::Relaxed) {
            // Safety: Guaranteed by caller.
            unsafe { self.write(entry, create) }
        }

        // Safety: The entry was initialized above.
        unsafe { (*entry.value.get()).assume_init_ref() }
    }

    /// Load the entry for the current thread, returning `None` if it has not
    /// been initialized.
    #[cfg(test)]
    fn try_load(&self) -> Option<&T> {
        let thread = Thread::current();

        // Safety: `thread.bucket` is always in bounds.
        let bucket_ptr =
            unsafe { self.buckets.get_unchecked(thread.bucket) }.load(Ordering::Acquire);

        if bucket_ptr.is_null() {
            return None;
        }

        // Safety: `thread.entry` is always in bounds, and we ensured the bucket was
        // initialized above.
        let entry = unsafe { &*bucket_ptr.add(thread.entry) };

        // Relaxed: Only the current thread can set the value.
        if !entry.present.load(Ordering::Relaxed) {
            return None;
        }

        // Safety: The entry was initialized above.
        unsafe { Some((*entry.value.get()).assume_init_ref()) }
    }

    /// Initialize the entry for the given thread.
    ///
    /// # Safety
    ///
    /// The current thread must have unique access to the uninitialized `entry`.
    #[cold]
    #[inline(never)]
    unsafe fn write(&self, entry: &Entry<T>, create: impl Fn() -> T) {
        // Insert the new element into the bucket.
        //
        // Safety: Guaranteed by caller.
        unsafe { entry.value.get().write(MaybeUninit::new(create())) };

        // Release: Necessary for synchronization with iterators.
        entry.present.store(true, Ordering::Release);

        // Synchronize with the heavy barrier in `retire`:
        // - If this fence comes first, the thread retiring will see our entry.
        // - If their barrier comes first, we will see the new values of any pointers
        //   being retired by that thread.
        //
        // Note that we do not use a light barrier here because the initialization of
        // the bucket is not performed with the light-store ordering. We
        // probably could avoid a full fence here, but there are no serious
        // performance implications.
        atomic::fence(Ordering::SeqCst);
    }

    // Initialize the bucket for the given thread's entry.
    #[cold]
    #[inline(never)]
    fn initialize(&self, bucket: &AtomicPtr<Entry<T>>, thread: Thread) -> *mut Entry<T> {
        let new_bucket = allocate_bucket(Thread::bucket_capacity(thread.bucket));

        match bucket.compare_exchange(
            ptr::null_mut(),
            new_bucket,
            // Release: If we win the race, synchronize with Acquire loads of the bucket from other
            // threads.
            Ordering::Release,
            // Acquire: If we lose the race, synchronize with the initialization of the bucket that
            // won.
            Ordering::Acquire,
        ) {
            // We won the race and initialized the bucket.
            Ok(_) => new_bucket,

            // We lost the race and can use the bucket that was stored instead.
            Err(other) => unsafe {
                // Safety: The pointer has not been shared.
                let _ = Box::from_raw(ptr::slice_from_raw_parts_mut(
                    new_bucket,
                    Thread::bucket_capacity(thread.bucket),
                ));

                other
            },
        }
    }

    /// Returns an iterator over all active thread slots.
    ///
    /// # Safety
    ///
    /// The values stored in the `ThreadLocal` by threads other than the current
    /// one must be sound to access.
    #[inline]
    pub unsafe fn iter(&self) -> Iter<'_, T> {
        Iter {
            index: 0,
            bucket: 0,
            thread_local: self,
            bucket_size: Thread::bucket_capacity(0),
        }
    }
}

impl<T> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        // Drop any buckets that were allocatec.
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let bucket_ptr = *bucket.get_mut();

            if bucket_ptr.is_null() {
                continue;
            }

            let bucket_size = Thread::bucket_capacity(i);

            // Safety: We have `&mut self` and ensured the bucket was initialized.
            let _ =
                unsafe { Box::from_raw(std::slice::from_raw_parts_mut(bucket_ptr, bucket_size)) };
        }
    }
}

impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        if *self.present.get_mut() {
            // Safety: We have `&mut self` and ensured the entry was initialized.
            unsafe {
                ptr::drop_in_place((*self.value.get()).as_mut_ptr());
            }
        }
    }
}

/// An iterator over a `ThreadLocal`.
pub struct Iter<'a, T> {
    bucket: usize,
    index: usize,
    bucket_size: usize,
    thread_local: &'a ThreadLocal<T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Because we reuse thread IDs, a new thread could join and be inserted into the
        // middle of the vector, meaning we have to check all the buckets here.
        // Yielding extra values is fine, but not yielding all originally active
        // threads is not.
        while self.bucket < thread_id::BUCKETS {
            // Safety: We ensured `self.bucket` was in-bounds above.
            let bucket = unsafe {
                self.thread_local
                    .buckets
                    .get_unchecked(self.bucket)
                    .load(Ordering::Acquire)
            };

            if !bucket.is_null() {
                while self.index < self.bucket_size {
                    // Safety: We ensured `self.index` was in-bounds above.
                    let entry = unsafe { &*bucket.add(self.index) };

                    // Advance to the next entry.
                    self.index += 1;

                    if entry.present.load(Ordering::Acquire) {
                        // Safety: We ensured the entry was initialized above, and the Acquire load
                        // ensures we synchronized with its initialization.
                        return Some(unsafe { (*entry.value.get()).assume_init_ref() });
                    }
                }
            }

            // Advance to the next bucket.
            self.index = 0;
            self.bucket += 1;
            self.bucket_size <<= 1;
        }

        None
    }
}

/// Allocate a bucket with the given capacity.
fn allocate_bucket<T>(capacity: usize) -> *mut Entry<T> {
    let entries = (0..capacity)
        .map(|_| Entry::<T> {
            present: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect::<Box<[Entry<T>]>>();

    Box::into_raw(entries) as *mut _
}

#[cfg(test)]
#[allow(clippy::redundant_closure)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn make_create() -> Arc<dyn Fn() -> usize + Send + Sync> {
        let count = AtomicUsize::new(0);
        Arc::new(move || count.fetch_add(1, Relaxed))
    }

    #[test]
    fn same_thread() {
        // Safety: Loading with `Thread::current` is always sound.
        unsafe {
            let create = make_create();
            let tls = ThreadLocal::with_capacity(1);
            assert_eq!(None, tls.try_load());
            assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
            assert_eq!(Some(&0), tls.try_load());
            assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
            assert_eq!(Some(&0), tls.try_load());
            assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
            assert_eq!(Some(&0), tls.try_load());
        }
    }

    #[test]
    fn different_thread() {
        // Safety: Loading with `Thread::current` is always sound.
        unsafe {
            let create = make_create();
            let tls = Arc::new(ThreadLocal::with_capacity(1));
            assert_eq!(None, tls.try_load());
            assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
            assert_eq!(Some(&0), tls.try_load());

            let tls2 = tls.clone();
            let create2 = create.clone();
            thread::spawn(move || {
                assert_eq!(None, tls2.try_load());
                assert_eq!(1, *tls2.load_or(|| create2(), Thread::current()));
                assert_eq!(Some(&1), tls2.try_load());
            })
            .join()
            .unwrap();

            assert_eq!(Some(&0), tls.try_load());
            assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
        }
    }

    #[test]
    fn iter() {
        // Safety: Loading with `Thread::current` is always sound.
        unsafe {
            let tls = Arc::new(ThreadLocal::with_capacity(1));
            tls.load_or(|| Box::new(1), Thread::current());

            let tls2 = tls.clone();
            thread::spawn(move || {
                tls2.load_or(|| Box::new(2), Thread::current());
                let tls3 = tls2.clone();
                thread::spawn(move || {
                    tls3.load_or(|| Box::new(3), Thread::current());
                })
                .join()
                .unwrap();
                drop(tls2);
            })
            .join()
            .unwrap();

            let tls = Arc::try_unwrap(tls).unwrap_or_else(|_| panic!("."));

            let mut v = tls.iter().map(|x| **x).collect::<Vec<i32>>();
            v.sort_unstable();
            assert_eq!(vec![1, 2, 3], v);
        }
    }

    #[test]
    fn iter_snapshot() {
        // Safety: Loading with `Thread::current` is always sound.
        unsafe {
            let tls = Arc::new(ThreadLocal::with_capacity(1));
            tls.load_or(|| Box::new(1), Thread::current());

            let iterator = tls.iter();
            tls.load_or(|| Box::new(2), Thread::current());

            let v = iterator.map(|x| **x).collect::<Vec<i32>>();
            assert_eq!(vec![1], v);
        }
    }

    #[test]
    fn test_drop() {
        let local = ThreadLocal::with_capacity(1);
        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        // Safety: Loading with `Thread::current` is always sound.
        unsafe {
            local.load_or(|| Dropped(dropped.clone()), Thread::current());
        }
        assert_eq!(dropped.load(Relaxed), 0);
        drop(local);
        assert_eq!(dropped.load(Relaxed), 1);
    }

    #[test]
    fn iter_many() {
        let tls = Arc::new(ThreadLocal::with_capacity(0));
        let barrier = Arc::new(Barrier::new(65));

        for i in 0..64 {
            let tls = tls.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                dbg!(i);
                // Safety: Loading with `Thread::current` is always sound.
                unsafe {
                    tls.load_or(|| 1, Thread::current());
                }
                barrier.wait();
            });
        }

        barrier.wait();
        unsafe { assert_eq!(tls.iter().count(), 64) }
    }
}
