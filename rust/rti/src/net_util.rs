/**
 * @file
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::io::Read;
use std::mem;
use std::net::TcpStream;

use crate::tag::Tag;

pub struct NetUtil {}

impl NetUtil {
    pub fn read_from_stream_errexit(
        stream: &mut TcpStream,
        buffer: &mut Vec<u8>,
        fed_id: u16,
        err_msg: &str,
    ) -> usize {
        let mut bytes_read = 0;
        while match stream.read(buffer) {
            Ok(msg_size) => {
                bytes_read = msg_size;
                false
            }
            Err(_) => {
                println!("RTI failed to read {} from federate {}.", err_msg, fed_id);
                // TODO: Implement similar to rti_lib.c
                std::process::exit(1);
            }
        } {}
        print!("  [[[ PACKET from {} ]]] = ", fed_id);
        for x in buffer {
            print!("{:02X?} ", x);
        }
        println!("\n");
        bytes_read
    }

    pub fn read_from_stream(stream: &mut TcpStream, buffer: &mut Vec<u8>, fed_id: u16) -> usize {
        let mut bytes_read = 0;
        while match stream.read(buffer) {
            Ok(msg_size) => {
                bytes_read = msg_size;
                false
            }
            Err(_) => {
                println!("ERROR reading the stream of federate {}.", fed_id);
                false
            }
        } {}
        print!("  [[[ BUFFER from {} ]]] = ", fed_id);
        for x in buffer {
            print!("{:02X?} ", x);
        }
        println!("\n");
        bytes_read
    }

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
        buffer[index] = (data & 0xff) as u8;
        buffer[index + 1] = ((data & 0xff00) >> 8) as u8;
        buffer[index + 2] = ((data & 0xff0000) >> 16) as u8;
        buffer[index + 3] = ((data & (0xff000000u32 as i32)) >> 24) as u8;
    }

    pub fn extract_timed_header(
        buffer: &[u8],
        port_id: &mut u16,
        federate_id: &mut u16,
        length: &mut i32,
        tag: &mut Tag,
    ) {
        Self::extract_header(buffer, port_id, federate_id, length);

        let start_idx =
            std::mem::size_of::<u16>() + std::mem::size_of::<u16>() + std::mem::size_of::<i32>();
        let temporary_tag = Self::extract_tag(&buffer[start_idx..]);
        tag.set_time(temporary_tag.time());
        tag.set_microstep(temporary_tag.microstep());
    }

    fn extract_header(buffer: &[u8], port_id: &mut u16, federate_id: &mut u16, length: &mut i32) {
        // The first two bytes are the ID of the destination reactor.
        let u16_size = std::mem::size_of::<u16>();
        // FIXME: Handle unwrap properly.
        *port_id = u16::from_le_bytes(buffer[0..u16_size].try_into().unwrap());

        // The next two bytes are the ID of the destination federate.
        // FIXME: Handle unwrap properly.
        *federate_id =
            u16::from_le_bytes(buffer[u16_size..(u16_size + u16_size)].try_into().unwrap());

        // The next four bytes are the message length.
        // FIXME: Handle unwrap properly.
        let local_length_signed = i32::from_le_bytes(
            buffer[(u16_size + u16_size)..(u16_size + u16_size + 4)]
                .try_into()
                .unwrap(),
        );
        if local_length_signed < 0 {
            println!(
                "Received an invalid message length ({}) from federate {}.",
                local_length_signed, *federate_id
            );
            // FIXME: Replace return to exit.
            return;
        }
        *length = local_length_signed;
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
        // println!("\ttime = ({}),  microstep = ({})", time, microstep);
        Tag::new(time, microstep)
    }
}
