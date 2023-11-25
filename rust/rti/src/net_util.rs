/**
 * @file
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::mem;

use crate::tag::Tag;

pub struct NetUtil {}

impl NetUtil {
    /**
     * Write the specified data as a sequence of bytes starting
     * at the specified address. This encodes the data in little-endian
     * order (lowest order byte first).
     * @param data The data to write.
     * @param buffer The whole byte vector to which the data is written.
     * @param index The location to start writing.
     */
    pub fn encode_int64(data: i64, buffer: &mut Vec<u8>, index: usize) {
        // This strategy is fairly brute force, but it avoids potential
        // alignment problems.
        let mut shift: i32 = 0;
        for i in 0..mem::size_of::<i64>() {
            buffer[index + i] = ((data & (0xff << shift)) >> shift) as u8;
            shift += 8;
        }
    }

    pub fn encode_int32(data: i32, buffer: &mut Vec<u8>, index: usize) {
        // This strategy is fairly brute force, but it avoids potential
        // alignment problems.  Note that this assumes an int32_t is four bytes.
        buffer[0] = (data & 0xff) as u8;
        buffer[1] = ((data & 0xff00) >> 8) as u8;
        buffer[2] = ((data & 0xff0000) >> 16) as u8;
        buffer[3] = ((data & (0xff000000u32 as i32)) >> 24) as u8;
    }

    pub fn extract_tag(buffer: &[u8]) -> Tag {
        // for x in buffer {
        //     print!("{:02X?} ", x);
        // }
        // print!("\n");
        // TODO: Exception handling of unwrap()
        let time = i64::from_le_bytes(buffer[0..mem::size_of::<i64>()].try_into().unwrap());
        let microstep = u32::from_le_bytes(
            buffer[mem::size_of::<i64>()..(mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        println!("\ttime = ({}),  microstep = ({})", time, microstep);
        Tag::new(time, microstep)
    }
}
