use crate::cfg::sync::atomic::{AtomicPtr, AtomicU64};
use crate::raw::Node;
use crate::utils::U64Padded;

use std::ops::{Index, IndexMut};
use std::ptr;

/// Represents a single protection slot.
///
/// This is useful for data structures like
/// linked lists that only have a single
/// node to protect.
///
/// # Examples
///
/// ```rust
/// // TODO
/// ```
pub struct SingleSlot;

unsafe impl Slots for SingleSlot {
    const SLOTS: usize = 1;

    type Arrays = Arrays<1>;

    fn as_index(self) -> usize {
        0
    }
}

/// A type representing protection slots in a collector.
///
/// Use the [`slots`] macro to generate a type implementing this trait.
pub unsafe trait Slots: Send + Sync {
    /// The number of reservation slots.
    const SLOTS: usize;

    /// A hack to work-around the limitations of const-generics.
    /// This type will be removed once the [`const_evaluatable_unchecked`]
    /// feature stabilizes.
    ///
    /// [`const_evaluatable_unchecked`]: https://github.com/rust-lang/rust/issues/76200
    type Arrays: internal::Arrays;

    /// Returns the slot that an instance of his type represents.
    fn as_index(self) -> usize;
}

/// Generates an enum where each variant represents a pointer protection slot.
///
/// Every local pointer that needs to be protected must use an individual
/// protection slot. Repeated calls to [`protect`](crate::Guard::protect) clear
/// previous reservations associated with the same slot. This allows
/// for finer grained memory reclamation. See the [usage guide](crate#guide)
/// for details.
///
/// # Example
///
/// ```rust
/// seize::slots! {
///     enum Slot {
///         HashTable,
///         BucketNode,
///     }
/// }
///
/// let collector: Collector<Slot> = Collector::new();
/// ```
#[macro_export]
macro_rules! slots {
    ($(#[$meta:meta])* $vis:vis enum $name:ident { $( $variant:ident ),+ $(,)? }) => {
        #[repr(usize)]
        $(#[$meta])*
        $vis enum $name {
            $( $variant, )+
        }

        const _: () = {
            unsafe impl ::seize::Slots for $name {
                const SLOTS: usize = [$($name::$variant),+].len();

                type Arrays = ::seize::Arrays<{ <$name as ::seize::Slots>::SLOTS }>;

                fn as_index(self) -> usize {
                    self as _
                }
            }
        };
    }
}

#[doc(hidden)]
pub struct Arrays<const N: usize>;

pub(crate) type Nodes<P> = <<P as Slots>::Arrays as internal::Arrays>::Nodes;
pub(crate) type Epochs<P> = <<P as Slots>::Arrays as internal::Arrays>::Epochs;
pub(crate) type AtomicNodes<P> = <<P as Slots>::Arrays as internal::Arrays>::AtomicNodes;

pub mod internal {
    use super::*;

    // This is a hack around the fact that we can't do [T; { <U as Trait>::N }]
    // on stable rust. We just declare arrays for each of the types we use
    pub trait Arrays {
        type Nodes: Array<*mut Node>;
        type Epochs: Array<AtomicU64>;
        type AtomicNodes: Array<U64Padded<AtomicPtr<Node>>>;
    }

    impl<const N: usize> Arrays for super::Arrays<N> {
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
        #[cfg(loom)]
        {
            Self(
                std::iter::repeat_with(|| AtomicU64::new(0))
                    .take(N)
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            )
        }

        #[cfg(not(loom))]
        {
            pub const ZERO: AtomicU64 = AtomicU64::new(0);
            Self([ZERO; N])
        }
    }
}

impl<const N: usize> Default for Array<U64Padded<AtomicPtr<Node>>, N> {
    fn default() -> Self {
        #[cfg(loom)]
        {
            Self(
                std::iter::repeat_with(|| U64Padded::new(AtomicPtr::new(Node::INACTIVE)))
                    .take(N)
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap_or_else(|_| panic!()),
            )
        }

        #[cfg(not(loom))]
        {
            pub const INACTIVE: U64Padded<AtomicPtr<Node>> =
                U64Padded::new(AtomicPtr::new(Node::INACTIVE));

            Self([INACTIVE; N])
        }
    }
}

unsafe impl<T, const N: usize> Send for Array<T, N> {}
unsafe impl<T, const N: usize> Sync for Array<T, N> {}
