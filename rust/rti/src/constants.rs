/**
 * @file
 * @author Hokeun Kim (hkim501@asu.edu)
 * @author Chanhee Lee (..)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::mem;

pub const MSG_TYPE_NEIGHBOR_STRUCTURE_HEADER_SIZE: i32 = 9;

pub const MSG_TYPE_TIMESTAMP_LENGTH: usize = 1 + mem::size_of::<i64>();

pub const STARTING_PORT: u16 = 15045;

pub const INET_ADDRSTRLEN: usize = 16;
