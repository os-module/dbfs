#![feature(error_in_core)]
#![no_std]

extern crate alloc;

mod fs;

pub use fs::{DbFileSystem, Dir, Error, File};

pub use jammdb;