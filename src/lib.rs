#![allow(clippy::missing_transmute_annotations)]
#![deny(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../README.md")]

mod collector;
mod deferred;
mod guard;
mod raw;
mod tls;
mod utils;

pub mod guide;
pub mod reclaim;

pub use collector::{AsLink, Collector, Link, Linked};
pub use deferred::Deferred;
pub use guard::{unprotected, Guard, LocalGuard, OwnedGuard, UnprotectedGuard};
