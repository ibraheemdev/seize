#![doc = include_str!("../README.md")]

mod cfg;
mod collector;
mod raw;
mod tls;
mod utils;

pub mod reclaim;

pub use collector::{Collector, Guard, Link, Linked};
