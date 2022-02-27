#![deny(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../README.md")]

mod cfg;
mod collector;
mod raw;
mod tls;
mod utils;
mod barrier;

pub mod reclaim;

pub use collector::{AtomicPtr, Collector, Guard, Link, Linked};
