/**
 * @file
 * @author Chanhee Lee (chanheel@asu.edu)
 * @author Hokeun Kim (hokeun@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::io::{Read, Write};
use std::mem;
use std::net::TcpStream;

use crate::tag::Tag;

pub struct NetUtil {}

impl NetUtil {
    pub fn read_from_socket_fail_on_error(
        stream: &mut TcpStream,
        buffer: &mut Vec<u8>,
        fed_id: u16,
        err_msg: &str,
    ) {
        while match stream.read(buffer) {
            Ok(..) => false,
            Err(_) => {
                println!("RTI failed to read {} from federate {}.", err_msg, fed_id);
                // TODO: Implement similarly with rti_lib.c
                std::process::exit(1);
            }
        } {}
    }

    pub fn read_from_socket(stream: &mut TcpStream, buffer: &mut Vec<u8>, fed_id: u16) -> usize {
        let mut bytes_read = 0;
        while match stream.read(buffer) {
            Ok(msg_size) => {
                bytes_read = msg_size;
                false
            }
            Err(_) => {
                println!("ERROR reading from the stream of federate {}.", fed_id);
                // TODO: Implement similarly with rti_lib.c
                false
            }
        } {}
        bytes_read
    }

    pub fn write_to_socket_fail_on_error(
        mut stream: &TcpStream,
        buffer: &Vec<u8>,
        fed_id: u16,
        err_msg: &str,
    ) {
        match stream.write(&buffer) {
            Ok(..) => {}
            Err(_e) => {
                println!("RTI failed to write {} to federate {}.", err_msg, fed_id);
                // TODO: Implement similarly with rti_lib.c
                std::process::exit(1);
            }
        }
    }

    pub fn write_to_socket(mut stream: &TcpStream, buffer: &Vec<u8>, fed_id: u16) -> usize {
        let mut bytes_written = 0;
        match stream.write(&buffer) {
            Ok(bytes_size) => {
                bytes_written = bytes_size;
            }
            Err(_e) => {
                println!("ERROR writing to the stream of federate {}.", fed_id);
                // TODO: Implement similarly with rti_lib.c
            }
        }
        bytes_written
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

#[cfg(test)]
mod tests {
    use super::*;

    use socket_server_mocker::server_mocker::ServerMocker;
    use socket_server_mocker::server_mocker_instruction::{
        ServerMockerInstruction, ServerMockerInstructionsList,
    };
    use socket_server_mocker::tcp_server_mocker::TcpServerMocker;

    use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};

    const MAX_BUFFER_SIZE: usize = 30000;
    const ERR_MESSAGE: &str = "test message";
    const I64_SIZE: usize = mem::size_of::<i64>();
    const I32_SIZE: usize = mem::size_of::<i32>();
    const LOCAL_HOST: &str = "127.0.0.1";

    #[test]
    fn test_read_from_socket_fail_on_error_positive() {
        let port_num = 35640;
        let tcp_server_mocker = TcpServerMocker::new(port_num).unwrap();
        let mut ip_address = LOCAL_HOST.to_owned();
        ip_address.push_str(":");
        ip_address.push_str(&port_num.to_string());
        let mut stream = TcpStream::connect(ip_address).unwrap();
        let mut rng = rand::thread_rng();
        let buffer_size: usize = rng.gen_range(0..MAX_BUFFER_SIZE);
        let msg = generate_random_bytes(buffer_size);
        let _ = tcp_server_mocker.add_mock_instructions_list(
            ServerMockerInstructionsList::new_with_instructions(
                [ServerMockerInstruction::SendMessage(msg.clone())].as_slice(),
            ),
        );
        let mut buffer = vec![0 as u8; buffer_size];
        NetUtil::read_from_socket_fail_on_error(&mut stream, &mut buffer, 0, ERR_MESSAGE);
        assert!(buffer == msg);
    }

    fn generate_random_bytes(buffer_size: usize) -> Vec<u8> {
        let seed = [0u8; 32];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        let mut bytes = vec![0 as u8; buffer_size];
        rng.fill_bytes(&mut bytes);
        bytes.to_vec()
    }

    #[test]
    fn test_read_from_socket_positive() {
        let port_num = 35642;
        let tcp_server_mocker = TcpServerMocker::new(port_num).unwrap();
        let mut ip_address = LOCAL_HOST.to_owned();
        ip_address.push_str(":");
        ip_address.push_str(&port_num.to_string());
        let mut stream = TcpStream::connect(ip_address).unwrap();
        let mut rng = rand::thread_rng();
        let buffer_size: usize = rng.gen_range(0..MAX_BUFFER_SIZE);
        let msg = generate_random_bytes(buffer_size);
        let _ = tcp_server_mocker.add_mock_instructions_list(
            ServerMockerInstructionsList::new_with_instructions(
                [ServerMockerInstruction::SendMessage(msg.clone())].as_slice(),
            ),
        );
        let mut buffer = vec![0 as u8; buffer_size];
        let read_size = NetUtil::read_from_socket(&mut stream, &mut buffer, 0);
        assert!(buffer == msg);
        assert!(buffer_size == read_size);
    }

    #[test]
    fn test_write_to_socket_fail_on_error_positive() {
        let port_num = 35644;
        let tcp_server_mocker = TcpServerMocker::new(port_num).unwrap();
        let mut ip_address = LOCAL_HOST.to_owned();
        ip_address.push_str(":");
        ip_address.push_str(&port_num.to_string());
        let mut stream = TcpStream::connect(ip_address).unwrap();
        let mut rng = rand::thread_rng();
        let buffer_size: usize = rng.gen_range(0..MAX_BUFFER_SIZE);
        let buffer = generate_random_bytes(buffer_size);
        let _ = NetUtil::write_to_socket_fail_on_error(&mut stream, &buffer, 0, ERR_MESSAGE);
        let _ = tcp_server_mocker.add_mock_instructions_list(
            ServerMockerInstructionsList::new_with_instructions(
                [ServerMockerInstruction::ReceiveMessage].as_slice(),
            ),
        );
        assert!(buffer == *tcp_server_mocker.pop_received_message().unwrap());
    }

    #[test]
    fn test_write_to_stream_positive() {
        let port_num = 35646;
        let tcp_server_mocker = TcpServerMocker::new(port_num).unwrap();
        let mut ip_address = LOCAL_HOST.to_owned();
        ip_address.push_str(":");
        ip_address.push_str(&port_num.to_string());
        let mut stream = TcpStream::connect(ip_address).unwrap();
        let mut rng = rand::thread_rng();
        let buffer_size: usize = rng.gen_range(0..MAX_BUFFER_SIZE);
        let buffer = generate_random_bytes(buffer_size);
        let written_size = NetUtil::write_to_socket(&mut stream, &buffer, 0);
        let _ = tcp_server_mocker.add_mock_instructions_list(
            ServerMockerInstructionsList::new_with_instructions(
                [ServerMockerInstruction::ReceiveMessage].as_slice(),
            ),
        );
        assert!(buffer == *tcp_server_mocker.pop_received_message().unwrap());
        assert!(buffer_size == written_size);
    }

    #[test]
    fn test_encode_int64_zero_index_positive() {
        let mut rng = rand::thread_rng();
        let value: i64 = rng.gen_range(0..i64::MAX);
        let mut buffer = vec![0 as u8; I64_SIZE];
        let _ = NetUtil::encode_int64(value, &mut buffer, 0);
        let i64_value = i64::from_le_bytes(buffer[0..I64_SIZE].try_into().unwrap());
        assert!(value == i64_value);
    }

    #[test]
    fn test_encode_int64_non_zero_index_positive() {
        let mut rng = rand::thread_rng();
        let value: i64 = rng.gen_range(0..i64::MAX);
        let idx: usize = rng.gen_range(0..I64_SIZE);
        let mut buffer = vec![0 as u8; idx + I64_SIZE];
        let _ = NetUtil::encode_int64(value, &mut buffer, idx);
        let i64_value = i64::from_le_bytes(buffer[idx..idx + I64_SIZE].try_into().unwrap());
        assert!(value == i64_value);
    }

    #[test]
    fn test_encode_int64_invalid_index_negative() {
        let mut rng = rand::thread_rng();
        let value: i64 = rng.gen_range(0..i64::MAX);
        let idx: usize = rng.gen_range(1..I64_SIZE);
        let mut buffer = vec![0 as u8; idx + I64_SIZE];
        let _ = NetUtil::encode_int64(value, &mut buffer, idx);
        let i64_value = i64::from_le_bytes(buffer[0..I64_SIZE].try_into().unwrap());
        assert!(value != i64_value);
    }

    #[test]
    fn test_encode_int32_zero_index_positive() {
        let mut rng = rand::thread_rng();
        let value: i32 = rng.gen_range(0..i32::MAX);
        let mut buffer = vec![0 as u8; I32_SIZE];
        let _ = NetUtil::encode_int32(value, &mut buffer, 0);
        let i32_value = i32::from_le_bytes(buffer[0..I32_SIZE].try_into().unwrap());
        assert!(value == i32_value);
    }

    #[test]
    fn test_encode_int32_non_zero_index_positive() {
        let mut rng = rand::thread_rng();
        let value: i32 = rng.gen_range(0..i32::MAX);
        let idx: usize = rng.gen_range(0..I64_SIZE);
        let mut buffer = vec![0 as u8; idx + I32_SIZE];
        let _ = NetUtil::encode_int32(value, &mut buffer, idx);
        let i32_value = i32::from_le_bytes(buffer[idx..idx + I32_SIZE].try_into().unwrap());
        assert!(value == i32_value);
    }

    #[test]
    fn test_encode_int32_invalid_index_negative() {
        let mut rng = rand::thread_rng();
        let value: i32 = rng.gen_range(0..i32::MAX);
        let idx: usize = rng.gen_range(1..I32_SIZE);
        let mut buffer = vec![0 as u8; idx + I32_SIZE];
        let _ = NetUtil::encode_int32(value, &mut buffer, idx);
        let i32_value = i32::from_le_bytes(buffer[0..I32_SIZE].try_into().unwrap());
        assert!(value != i32_value);
    }

    #[test]
    pub fn test_extract_timed_header_positive() {
        let buffer_size = mem::size_of::<u16>() * 2
            + mem::size_of::<i32>()
            + mem::size_of::<i64>()
            + mem::size_of::<u32>();
        let mut buffer = vec![0 as u8; buffer_size];
        let mut rng = rand::thread_rng();
        let port_value: u16 = rng.gen_range(1..u16::MAX);
        let federate_id_value: u16 = rng.gen_range(1..u16::MAX);
        let local_lenth_signed_value: i32 = rng.gen_range(1..i32::MAX);
        let time_value: i64 = rng.gen_range(1..i64::MAX);
        let microstep_value: u32 = rng.gen_range(1..u32::MAX);
        let mut idx = 0;
        for val in port_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in federate_id_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in local_lenth_signed_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in time_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in microstep_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        let mut port = 0;
        let mut federate_id = 0;
        let mut local_length_signed = 0;
        let mut tag = Tag::new(0, 0);
        NetUtil::extract_timed_header(
            &buffer,
            &mut port,
            &mut federate_id,
            &mut local_length_signed,
            &mut tag,
        );
        assert!(port == port_value);
        assert!(federate_id == federate_id_value);
        assert!(local_length_signed == local_lenth_signed_value);
        assert!(tag.time() == time_value);
        assert!(tag.microstep() == microstep_value);
    }

    #[test]
    pub fn test_extract_timed_header_negative_local_length_negative() {
        let buffer_size = mem::size_of::<u16>() * 2
            + mem::size_of::<i32>()
            + mem::size_of::<i64>()
            + mem::size_of::<u32>();
        let mut buffer = vec![0 as u8; buffer_size];
        let mut rng = rand::thread_rng();
        let port_value: u16 = rng.gen_range(1..u16::MAX);
        let federate_id_value: u16 = rng.gen_range(1..u16::MAX);
        let local_lenth_signed_value: i32 = rng.gen_range(1..i32::MAX - 1) * (-1);
        let time_value: i64 = rng.gen_range(1..i64::MAX);
        let microstep_value: u32 = rng.gen_range(1..u32::MAX);
        let mut idx = 0;
        for val in port_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in federate_id_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in local_lenth_signed_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in time_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        for val in microstep_value.to_le_bytes() {
            buffer[idx] = val;
            idx += 1;
        }
        let mut port = 0;
        let mut federate_id = 0;
        let mut local_length_signed = 0;
        let mut tag = Tag::new(0, 0);
        NetUtil::extract_timed_header(
            &buffer,
            &mut port,
            &mut federate_id,
            &mut local_length_signed,
            &mut tag,
        );
        assert!(port == port_value);
        assert!(federate_id == federate_id_value);
        assert!(local_length_signed == 0);
    }
}
