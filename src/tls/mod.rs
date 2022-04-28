// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

mod thread_id;

use crate::utils::{self, NoDropGlue, Zeroable};

use std::mem;
use std::ptr;
use std::sync::atomic::AtomicUsize;
// TODO: loom doesn't expose get_mut
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

const BUCKETS: usize = (usize::BITS + 1) as usize;

pub struct ThreadLocal<T: Send> {
    entries: AtomicUsize,
    buckets: [AtomicPtr<Entry<T>>; BUCKETS],
}

struct Entry<T> {
    active: AtomicBool,
    value: T,
}

unsafe impl<T: Zeroable> Zeroable for Entry<T> {}

unsafe impl<T: Send> Sync for ThreadLocal<T> {}

impl<T> ThreadLocal<T>
where
    T: Zeroable + NoDropGlue + Send,
{
    pub fn with_capacity(capacity: usize) -> ThreadLocal<T> {
        let allocated_buckets = capacity
            .checked_sub(1)
            .map(|c| (usize::BITS as usize) - (c.leading_zeros() as usize) + 1)
            .unwrap_or(0);

        let mut buckets = [ptr::null_mut(); BUCKETS];
        let mut bucket_size = 1;
        for (i, bucket) in buckets[..allocated_buckets].iter_mut().enumerate() {
            *bucket = allocate_bucket::<T>(bucket_size);

            if i != 0 {
                bucket_size <<= 1;
            }
        }

        ThreadLocal {
            entries: AtomicUsize::new(0),
            // safety: `AtomicPtr` has the same representation as a pointer
            buckets: unsafe { mem::transmute(buckets) },
        }
    }

    pub fn load_or_zeroed(&self) -> &T {
        let thread = thread_id::get();

        let bucket = unsafe { self.buckets.get_unchecked(thread.bucket) };
        let mut bucket_ptr = bucket.load(Ordering::Acquire);

        if bucket_ptr.is_null() {
            let new_bucket = allocate_bucket(thread.bucket_size);

            match bucket.compare_exchange(
                ptr::null_mut(),
                new_bucket,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => bucket_ptr = new_bucket,
                // if the bucket value changed (from null), that means
                // another thread stored a new bucket before we could,
                // and we can free our bucket and use that one instead
                Err(other) => unsafe {
                    let _ = Box::from_raw(new_bucket);
                    bucket_ptr = other;
                },
            }
        }

        unsafe {
            let entry = bucket_ptr.add(thread.index);

            if !(*entry).active.load(Ordering::Relaxed) {
                (*entry).active.store(true, Ordering::Relaxed);

                // release: make the store of entry.active
                // visible to iterators
                self.entries.fetch_add(1, Ordering::Release);
            }

            &(*entry).value
        }
    }

    pub fn entries(&self) -> usize {
        // acquire: acquire any stores of entry.active
        // (used by iterators)
        self.entries.load(Ordering::Acquire)
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            bucket: 0,
            bucket_size: 1,
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
        let mut bucket_size = 1;

        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let bucket_ptr = *bucket.get_mut();

            let this_bucket_size = bucket_size;
            if i != 0 {
                bucket_size <<= 1;
            }

            if bucket_ptr.is_null() {
                continue;
            }

            unsafe { Box::from_raw(std::slice::from_raw_parts_mut(bucket_ptr, this_bucket_size)) };
        }
    }
}

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
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        // we have to check all the buckets here
        // because we reuse thread IDs. keeping track
        // of the number of values and only yielding
        // that many here wouldn't work, because a new
        // thread could join and be inserted into a middle
        // bucket, and we would yield that instead of an
        // active thread that actually needs to participate
        // in reference counting. yielding extra values is
        // fine, but not yielding all originally active
        // threads is not.
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
                    if entry.active.load(Ordering::Relaxed) {
                        return Some(&entry.value);
                    }
                }
            }

            if self.bucket != 0 {
                self.bucket_size <<= 1;
            }

            self.bucket += 1;
            self.index = 0;
        }

        None
    }
}

fn allocate_bucket<T: Zeroable>(size: usize) -> *mut Entry<T> {
    Box::into_raw(utils::alloc_zeroed_slice::<Entry<T>>(size)) as *mut _
}

#[cfg(test)]
#[allow(clippy::redundant_closure)]
mod tests {
    use super::ThreadLocal;

    use std::cell::{RefCell, UnsafeCell};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn same_thread() {
        let tls = ThreadLocal::<UnsafeCell<u64>>::with_capacity(1);
        unsafe {
            assert_eq!(0, *tls.load_or_zeroed().get());
            *tls.load_or_zeroed().get() = 1;
            assert_eq!(1, *tls.load_or_zeroed().get());
        }
    }

    #[test]
    fn different_thread() {
        let tls = Arc::new(ThreadLocal::<UnsafeCell<u64>>::with_capacity(1));
        unsafe {
            assert_eq!(0, *tls.load_or_zeroed().get());
            *tls.load_or_zeroed().get() = 1;
        }

        let tls2 = tls.clone();
        thread::spawn(move || unsafe {
            assert_eq!(0, *tls2.load_or_zeroed().get());
            *tls2.load_or_zeroed().get() = 2;
        })
        .join()
        .unwrap();

        unsafe { assert_eq!(1, *tls.load_or_zeroed().get()) }
    }

    #[test]
    fn iter() {
        let tls = Arc::new(ThreadLocal::<UnsafeCell<u64>>::with_capacity(1));
        unsafe { *tls.load_or_zeroed().get() = 1 }

        let tls2 = tls.clone();
        thread::spawn(move || unsafe {
            *tls2.load_or_zeroed().get() = 2;
            let tls3 = tls2.clone();
            thread::spawn(move || {
                *tls3.load_or_zeroed().get() = 3;
            })
            .join()
            .unwrap();
            drop(tls2);
        })
        .join()
        .unwrap();

        let tls = Arc::try_unwrap(tls).unwrap_or_else(|_| panic!("."));

        let mut v = tls.iter().map(|x| unsafe { *x.get() }).collect::<Vec<_>>();
        v.sort_unstable();
        assert_eq!(vec![1, 2, 3], v);
    }

    #[test]
    fn iter_snapshot() {
        let tls = Arc::new(ThreadLocal::<UnsafeCell<u64>>::with_capacity(1));
        unsafe { *tls.load_or_zeroed().get() = 1 }

        let iterator = tls.iter();
        let tls2 = tls.clone();
        thread::spawn(move || unsafe { *tls2.load_or_zeroed().get() = 2 })
            .join()
            .unwrap();

        let mut v = iterator.map(|x| unsafe { *x.get() }).collect::<Vec<_>>();
        v.sort_unstable();
        assert_eq!(vec![1, 2], v);
    }

    #[test]
    fn is_sync() {
        fn foo<T: Sync>() {}
        foo::<ThreadLocal<String>>();
        foo::<ThreadLocal<RefCell<String>>>();
    }
}
