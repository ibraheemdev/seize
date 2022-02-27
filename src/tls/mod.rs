// Copyright 2017 Amanieu d'Antras
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

mod thread_id;
use thread_id::Thread;

use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;

// TODO: loom doesn't expose get_mut
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

const BUCKETS: usize = (usize::BITS + 1) as usize;

pub struct ThreadLocal<T: Send> {
    buckets: [AtomicPtr<Entry<T>>; BUCKETS],
}

struct Entry<T> {
    present: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
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

unsafe impl<T: Send> Sync for ThreadLocal<T> {}

impl<T> ThreadLocal<T>
where
    T: Send,
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
            // Safety: AtomicPtr has the same representation as a pointer and arrays have the same
            // representation as a sequence of their inner type.
            buckets: unsafe { mem::transmute(buckets) },
        }
    }

    pub fn get_or(&self, f: impl Fn() -> T) -> &T {
        let thread = thread_id::get();

        match self.get_inner(thread) {
            Some(x) => x,
            None => self.insert(thread, f()),
        }
    }

    #[cfg(test)]
    pub fn get(&self) -> Option<&T> {
        self.get_inner(thread_id::get())
    }

    fn get_inner(&self, thread: Thread) -> Option<&T> {
        let bucket_ptr =
            unsafe { self.buckets.get_unchecked(thread.bucket) }.load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            return None;
        }
        unsafe {
            let entry = &*bucket_ptr.add(thread.index);
            // Read without atomic operations as only this thread can set the value.
            if (&entry.present as *const _ as *const bool).read() {
                Some(&*(&*entry.value.get()).as_ptr())
            } else {
                None
            }
        }
    }

    #[cold]
    fn insert(&self, thread: Thread, data: T) -> &T {
        let bucket = unsafe { self.buckets.get_unchecked(thread.bucket) };

        let bucket_ptr: *const _ = bucket.load(Ordering::Acquire);

        // If the bucket doesn't already exist, we need to allocate it
        let bucket_ptr = if bucket_ptr.is_null() {
            let new_bucket = allocate_bucket(thread.bucket_size);

            match bucket.compare_exchange(
                ptr::null_mut(),
                new_bucket,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => new_bucket,
                // If the bucket value changed (from null), that means
                // another thread stored a new bucket before we could,
                // and we can free our bucket and use that one instead
                Err(bucket_ptr) => {
                    unsafe {
                        let _ = Box::from_raw(new_bucket);
                    }

                    bucket_ptr
                }
            }
        } else {
            bucket_ptr
        };

        // Insert the new element into the bucket
        let entry = unsafe { &*bucket_ptr.add(thread.index) };
        let value_ptr = entry.value.get();
        unsafe { value_ptr.write(MaybeUninit::new(data)) };
        entry.present.store(true, Ordering::Release);

        unsafe { &*(&*value_ptr).as_ptr() }
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
        // We have to check all the buckets here
        // because we reuse thread IDs. Keeping track
        // of the number of values and only yielding
        // that many here wouldn't work, because a new
        // thread could join and be inserted into a middle
        // bucket, and we would yield that instead of an
        // active thread that actually needs to participate
        // in reference counting. Yielding extra values is
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
                    if entry.present.load(Ordering::Acquire) {
                        return Some(unsafe { &*(&*entry.value.get()).as_ptr() });
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

fn allocate_bucket<T>(size: usize) -> *mut Entry<T> {
    Box::into_raw(
        (0..size)
            .map(|_| Entry::<T> {
                present: AtomicBool::new(false),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect(),
    ) as *mut _
}

#[cfg(test)]
#[allow(clippy::redundant_closure)]
mod tests {
    use super::ThreadLocal;
    use std::cell::RefCell;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use std::thread;

    fn make_create() -> Arc<dyn Fn() -> usize + Send + Sync> {
        let count = AtomicUsize::new(0);
        Arc::new(move || count.fetch_add(1, Relaxed))
    }

    #[test]
    fn same_thread() {
        let create = make_create();
        let tls = ThreadLocal::with_capacity(1);
        assert_eq!(None, tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());
    }

    #[test]
    fn different_thread() {
        let create = make_create();
        let tls = Arc::new(ThreadLocal::with_capacity(1));
        assert_eq!(None, tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
        assert_eq!(Some(&0), tls.get());

        let tls2 = tls.clone();
        let create2 = create.clone();
        thread::spawn(move || {
            assert_eq!(None, tls2.get());
            assert_eq!(1, *tls2.get_or(|| create2()));
            assert_eq!(Some(&1), tls2.get());
        })
        .join()
        .unwrap();

        assert_eq!(Some(&0), tls.get());
        assert_eq!(0, *tls.get_or(|| create()));
    }

    #[test]
    fn iter() {
        let tls = Arc::new(ThreadLocal::with_capacity(1));
        tls.get_or(|| Box::new(1));

        let tls2 = tls.clone();
        thread::spawn(move || {
            tls2.get_or(|| Box::new(2));
            let tls3 = tls2.clone();
            thread::spawn(move || {
                tls3.get_or(|| Box::new(3));
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

    #[test]
    fn iter_snapshot() {
        let tls = Arc::new(ThreadLocal::with_capacity(1));
        tls.get_or(|| Box::new(1));

        let iterator = tls.iter();
        tls.get_or(|| Box::new(2));

        let v = iterator.map(|x| **x).collect::<Vec<i32>>();
        assert_eq!(vec![1], v);
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
        local.get_or(|| Dropped(dropped.clone()));
        assert_eq!(dropped.load(Relaxed), 0);
        drop(local);
        assert_eq!(dropped.load(Relaxed), 1);
    }

    #[test]
    fn is_sync() {
        fn foo<T: Sync>() {}
        foo::<ThreadLocal<String>>();
        foo::<ThreadLocal<RefCell<String>>>();
    }
}
