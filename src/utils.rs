/// Pads and aligns a value to the length of a cache line.
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "riscv64",
    ),
    repr(align(32))
)]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "riscv64",
        target_arch = "s390x",
    )),
    repr(align(64))
)]
#[derive(Default)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// Pads a pointer to 64 bits.
pub struct U64Padded<T> {
    ptr: T,
    _pad: [u8; std::mem::size_of::<u64>() - std::mem::size_of::<*mut ()>()],
}

impl<T> U64Padded<T> {
    pub const fn new(value: T) -> Self {
        Self {
            ptr: value,
            _pad: [0; std::mem::size_of::<u64>() - std::mem::size_of::<*mut ()>()],
        }
    }
}

impl<T> std::ops::Deref for U64Padded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<T> std::ops::DerefMut for U64Padded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ptr
    }
}

macro_rules! const_assert {
    ($x:expr $(,)?) => {
        #[allow(unknown_lints, eq_op)]
        const _: [(); 0 - !{
            const ASSERT: bool = $x;
            ASSERT
        } as usize] = [];
    };
}

pub(crate) use const_assert;
