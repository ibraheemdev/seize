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

use crate::utils::CachePadded;

// Skip smaller buckets to avoid unnecessary allocations.
const SKIP: usize = 32;
const SKIP_BUCKET: usize = ((usize::BITS - SKIP.leading_zeros()) as usize) - 1;

const BUCKETS: usize = (usize::BITS as usize) - SKIP_BUCKET;

// Per-object thread local storage.
pub struct ThreadLocal<T: Send> {
    buckets: [AtomicPtr<Entry<T>>; BUCKETS],
}

struct Entry<T> {
    present: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Sync for ThreadLocal<T> {}

impl<T> ThreadLocal<CachePadded<T>>
where
    T: Send,
{
    /// Load the slot for the given `thread`, initializing it with a default
    /// value if necessary.
    #[inline]
    pub fn load(&self, thread: Thread) -> *mut T
    where
        T: Default,
    {
        let padded = self.load_or(CachePadded::<T>::default, thread);
        unsafe { ptr::addr_of_mut!((*padded).value) }
    }
}

impl<T> ThreadLocal<T>
where
    T: Send,
{
    /// Create a `ThreadLocal` container with the given initial capacity.
    pub fn with_capacity(capacity: usize) -> ThreadLocal<T> {
        let init = match capacity {
            0 => 0,
            // Initialize enough buckets for `capacity` elements.
            n => Thread::new(n).bucket,
        };

        let mut buckets = [ptr::null_mut(); BUCKETS];
        for (i, bucket) in buckets[..=init].iter_mut().enumerate() {
            let bucket_size = Thread::bucket_size(i);
            *bucket = allocate_bucket::<T>(bucket_size);
        }

        ThreadLocal {
            // Safety: `AtomicPtr` has the same representation as a pointer.
            buckets: unsafe { mem::transmute(buckets) },
        }
    }

    /// Load the entry for the given `thread`, initializing it using the provided
    /// function if necessary.
    #[inline]
    pub fn load_or(&self, create: impl Fn() -> T, thread: Thread) -> *mut T {
        let bucket = unsafe { self.buckets.get_unchecked(thread.bucket) };
        let mut bucket_ptr = bucket.load(Ordering::Acquire);

        if bucket_ptr.is_null() {
            bucket_ptr = self.initialize(bucket, thread);
        }

        unsafe {
            let entry = &*bucket_ptr.add(thread.index);

            // Relaxed: Only the current thread can set the value.
            if !entry.present.load(Ordering::Relaxed) {
                self.write(entry, create)
            }

            // Safety: The entry was initialized above.
            entry.value.get().cast()
        }
    }

    // Initialize the entry for the given thread.
    //
    // # Safety
    //
    // The thread must have unique access to the uninitialized `entry`.
    #[cold]
    #[inline(never)]
    unsafe fn write(&self, entry: &Entry<T>, create: impl Fn() -> T) {
        // Insert the new element into the bucket.
        unsafe { entry.value.get().write(MaybeUninit::new(create())) };

        // Release: Necessary for iterators.
        entry.present.store(true, Ordering::Release);

        // Synchronize with the heavy barrier in `retire`:
        // - If this fence comes first, the thread retiring will see our entry.
        // - If their barrier comes first, we will see the new values of any pointers
        //   being retired by that thread.
        atomic::fence(Ordering::SeqCst);
    }

    // Initialize the entry for the given thread.
    #[cold]
    #[inline(never)]
    fn initialize(&self, bucket: &AtomicPtr<Entry<T>>, thread: Thread) -> *mut Entry<T> {
        let new_bucket = allocate_bucket(Thread::bucket_size(thread.bucket));

        match bucket.compare_exchange(
            ptr::null_mut(),
            new_bucket,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => new_bucket,
            // If the bucket value changed (from null), that means
            // another thread stored a new bucket before we could,
            // and we can free our bucket and use that one instead.
            Err(other) => unsafe {
                let _ = Box::from_raw(ptr::slice_from_raw_parts_mut(
                    new_bucket,
                    Thread::bucket_size(thread.bucket),
                ));

                other
            },
        }
    }

    /// Load the
    #[cfg(test)]
    fn try_load(&self) -> Option<&T> {
        let thread = Thread::current();
        let bucket_ptr =
            unsafe { self.buckets.get_unchecked(thread.bucket) }.load(Ordering::Acquire);

        if bucket_ptr.is_null() {
            return None;
        }

        unsafe {
            let entry = &*bucket_ptr.add(thread.index);
            // Relaxed: Only the current thread can set the value.
            if entry.present.load(Ordering::Relaxed) {
                Some((*entry.value.get()).assume_init_ref())
            } else {
                None
            }
        }
    }

    /// Returns an iterator over all active thread slots.
    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            bucket: 0,
            bucket_size: Thread::bucket_size(0),
            index: 0,
            thread_local: self,
        }
    }
}

impl<T> Drop for ThreadLocal<T>
where
    T: Send,
{
    fn drop(&mut self) {
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let bucket_ptr = *bucket.get_mut();

            if bucket_ptr.is_null() {
                continue;
            }

            let bucket_size = Thread::bucket_size(i);
            let _ =
                unsafe { Box::from_raw(std::slice::from_raw_parts_mut(bucket_ptr, bucket_size)) };
        }
    }
}

impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        unsafe {
            if *self.present.get_mut() {
                ptr::drop_in_place((*self.value.get()).as_mut_ptr());
            }
        }
    }
}

/// An iterator over a `ThreadLocal`.
pub struct Iter<'a, T>
where
    T: Send,
{
    thread_local: &'a ThreadLocal<T>,
    bucket: usize,
    bucket_size: usize,
    index: usize,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Send,
{
    type Item = *mut T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Because we reuse thread IDs, a new thread could join and be inserted into the
        // middle of the list, meaning we have to check all the buckets here. Yielding
        // extra values is fine, but not yielding all originally active threads
        // is not.
        while self.bucket < BUCKETS {
            let bucket = unsafe {
                self.thread_local
                    .buckets
                    .get_unchecked(self.bucket)
                    .load(Ordering::Acquire)
            };

            if !bucket.is_null() {
                while self.index < self.bucket_size {
                    let entry = unsafe { &*bucket.add(self.index) };
                    self.index += 1;
                    if entry.present.load(Ordering::Acquire) {
                        return Some(entry.value.get().cast());
                    }
                }
            }

            self.bucket_size <<= 1;
            self.bucket += 1;
            self.index = 0;
        }

        None
    }
}

/// Allocate a bucket of the given size.
fn allocate_bucket<T>(size: usize) -> *mut Entry<T> {
    let entries = (0..size)
        .map(|_| Entry::<T> {
            present: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect::<Box<[Entry<T>]>>();

    Box::into_raw(entries) as *mut _
}

// #[cfg(test)]
// #[allow(clippy::redundant_closure)]
// mod tests {
//     use super::*;
//
//     use std::cell::RefCell;
//     use std::sync::atomic::AtomicUsize;
//     use std::sync::atomic::Ordering::Relaxed;
//     use std::sync::{Arc, Barrier};
//     use std::thread;
//
//     fn make_create() -> Arc<dyn Fn() -> usize + Send + Sync> {
//         let count = AtomicUsize::new(0);
//         Arc::new(move || count.fetch_add(1, Relaxed))
//     }
//
//     #[test]
//     fn same_thread() {
//         let create = make_create();
//         let tls = ThreadLocal::with_capacity(1);
//         assert_eq!(None, tls.try_load());
//         assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
//         assert_eq!(Some(&0), tls.try_load());
//         assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
//         assert_eq!(Some(&0), tls.try_load());
//         assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
//         assert_eq!(Some(&0), tls.try_load());
//     }
//
//     #[test]
//     fn different_thread() {
//         let create = make_create();
//         let tls = Arc::new(ThreadLocal::with_capacity(1));
//         assert_eq!(None, tls.try_load());
//         assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
//         assert_eq!(Some(&0), tls.try_load());
//
//         let tls2 = tls.clone();
//         let create2 = create.clone();
//         thread::spawn(move || {
//             assert_eq!(None, tls2.try_load());
//             assert_eq!(1, *tls2.load_or(|| create2(), Thread::current()));
//             assert_eq!(Some(&1), tls2.try_load());
//         })
//         .join()
//         .unwrap();
//
//         assert_eq!(Some(&0), tls.try_load());
//         assert_eq!(0, *tls.load_or(|| create(), Thread::current()));
//     }
//
//     #[test]
//     fn iter() {
//         let tls = Arc::new(ThreadLocal::with_capacity(1));
//         tls.load_or(|| Box::new(1), Thread::current());
//
//         let tls2 = tls.clone();
//         thread::spawn(move || {
//             tls2.load_or(|| Box::new(2), Thread::current());
//             let tls3 = tls2.clone();
//             thread::spawn(move || {
//                 tls3.load_or(|| Box::new(3), Thread::current());
//             })
//             .join()
//             .unwrap();
//             drop(tls2);
//         })
//         .join()
//         .unwrap();
//
//         let tls = Arc::try_unwrap(tls).unwrap_or_else(|_| panic!("."));
//
//         let mut v = tls.iter().map(|x| **x).collect::<Vec<i32>>();
//         v.sort_unstable();
//         assert_eq!(vec![1, 2, 3], v);
//     }
//
//     #[test]
//     fn iter_snapshot() {
//         let tls = Arc::new(ThreadLocal::with_capacity(1));
//         tls.load_or(|| Box::new(1), Thread::current());
//
//         let iterator = tls.iter();
//         tls.load_or(|| Box::new(2), Thread::current());
//
//         let v = iterator.map(|x| **x).collect::<Vec<i32>>();
//         assert_eq!(vec![1], v);
//     }
//
//     #[test]
//     fn test_drop() {
//         let local = ThreadLocal::with_capacity(1);
//         struct Dropped(Arc<AtomicUsize>);
//         impl Drop for Dropped {
//             fn drop(&mut self) {
//                 self.0.fetch_add(1, Relaxed);
//             }
//         }
//
//         let dropped = Arc::new(AtomicUsize::new(0));
//         local.load_or(|| Dropped(dropped.clone()), Thread::current());
//         assert_eq!(dropped.load(Relaxed), 0);
//         drop(local);
//         assert_eq!(dropped.load(Relaxed), 1);
//     }
//
//     #[test]
//     fn iter_many() {
//         let tls = Arc::new(ThreadLocal::with_capacity(0));
//         let barrier = Arc::new(Barrier::new(65));
//
//         for i in 0..64 {
//             let tls = tls.clone();
//             let barrier = barrier.clone();
//             thread::spawn(move || {
//                 dbg!(i);
//                 tls.load_or(|| 1, Thread::current());
//                 barrier.wait();
//             });
//         }
//
//         barrier.wait();
//         assert_eq!(tls.iter().count(), 64);
//     }
//
//     #[test]
//     fn is_sync() {
//         fn foo<T: Sync>() {}
//         foo::<ThreadLocal<String>>();
//         foo::<ThreadLocal<RefCell<String>>>();
//     }
// }
