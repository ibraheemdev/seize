#![deny(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../README.md")]

mod collector;
mod guard;
mod raw;
mod tls;
mod utils;

pub mod reclaim;

pub use collector::{AsLink, Collector, Link, Linked};
pub use guard::{unprotected, Guard, LocalGuard, OwnedGuard, UnprotectedGuard};
