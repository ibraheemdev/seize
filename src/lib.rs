#![deny(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../README.md")]

mod collector;
mod raw;
mod tls;
mod utils;

pub mod reclaim;

pub use collector::{AsLink, Collector, Guard, Link, Linked};
pub use tls::Thread;
