#![recursion_limit = "512"]
#![feature(error_in_core)]

#![no_std]


extern crate alloc;

mod fs;


pub use fs::{DirEntry,FileSystem,File};