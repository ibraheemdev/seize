use crate::raw::Node;
use crate::utils::U64Padded;

use std::ops::{Index, IndexMut};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64};

pub unsafe trait Protect: Send + Sync {
    const SLOTS: usize;

    type Slots: internal::Slots;

    fn as_index(self) -> usize;
}

#[macro_export]
macro_rules! protection {
    ($(#[$meta:meta])* $vis:vis enum $name:ident { $( $variant:ident ),+ $(,)? }) => {
        #[repr(usize)]
        $(#[$meta])*
        $vis enum $name {
            $( $variant, )+
        }

        const _: () = {
            unsafe impl ::seize::Protect for $name {
                const SLOTS: usize = [$($name::$variant),+].len();
                type Slots = ::seize::Slots<{ <$name as ::seize::Protect>::SLOTS }>;

                fn as_index(self) -> usize {
                    self as _
                }
            }
        };
    }
}

pub struct Slots<const N: usize>;

pub(crate) type Nodes<P> = <<P as Protect>::Slots as internal::Slots>::Nodes;
pub(crate) type Epochs<P> = <<P as Protect>::Slots as internal::Slots>::Epochs;
pub(crate) type AtomicNodes<P> = <<P as Protect>::Slots as internal::Slots>::AtomicNodes;

pub mod internal {
    use super::*;

    // This is a hack around the fact that we can't do [T; { <U as Trait>::N }]
    // on stable rust. We just declare arrays for each of the types we use
    pub trait Slots {
        type Nodes: Array<*mut Node>;
        type Epochs: Array<AtomicU64>;
        type AtomicNodes: Array<U64Padded<AtomicPtr<Node>>>;
    }

    impl<const N: usize> Slots for super::Slots<N> {
        type Nodes = super::Array<*mut Node, N>;
        type Epochs = super::Array<AtomicU64, N>;
        type AtomicNodes = super::Array<U64Padded<AtomicPtr<Node>>, N>;
    }

    pub trait Array<T>
    where
        Self: Index<usize, Output = T> + IndexMut<usize> + Send + Sync + Default,
    {
    }

    impl<T, const N: usize> Array<T> for super::Array<T, N> where Self: Default {}
}

pub struct Array<T, const N: usize>([T; N]);

impl<T, const N: usize> Index<usize> for Array<T, N> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl<T, const N: usize> IndexMut<usize> for Array<T, N> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<const N: usize> Default for Array<*mut Node, N> {
    fn default() -> Self {
        Self([ptr::null_mut(); N])
    }
}

impl<const N: usize> Default for Array<AtomicU64, N> {
    fn default() -> Self {
        pub const ZERO: AtomicU64 = AtomicU64::new(0);

        Self([ZERO; N])
    }
}

impl<const N: usize> Default for Array<U64Padded<AtomicPtr<Node>>, N> {
    fn default() -> Self {
        pub const INACTIVE: U64Padded<AtomicPtr<Node>> =
            U64Padded::new(AtomicPtr::new(Node::INACTIVE));

        Self([INACTIVE; N])
    }
}

unsafe impl<T, const N: usize> Send for Array<T, N> {}
unsafe impl<T, const N: usize> Sync for Array<T, N> {}
