use crate::Link;

pub unsafe fn boxed<T>(mut link: Link) {
    let _ = Box::from_raw(link.as_ptr::<T>());
}

pub unsafe fn in_place<T>(mut link: Link) {
    let _ = std::ptr::drop_in_place(link.as_ptr::<T>());
}
