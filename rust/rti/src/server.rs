/**
 * @file
 * @author Hokeun Kim (hkim501@asu.edu)
 * @author Chanhee Lee (..)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

use crate::constants::*;
use crate::net_common::{ErrType, MsgType};

pub struct Server {
    ip_v4: String,
    port: String,
}

impl Server {
    pub fn new(ip_v4: String, port: String) -> Server {
        Server { ip_v4, port }
    }

    pub fn listen(self) {
        let mut address = self.ip_v4;
        address.push(':');
        address.push_str(self.port.as_str());
        let listener = TcpListener::bind(address).unwrap();
        // accept connections and process them, spawning a new thread for each one
        println!("Server listening on port 15045");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("New connection: {}", stream.peer_addr().unwrap());
                    thread::spawn(move || {
                        // connection succeeded
                        Self::handle_client(stream)
                    });
                }
                Err(e) => {
                    println!("Error: {} during stream handling", e);
                    /* connection failed */
                }
            }
        }

        // close the socket server
        drop(listener);
    }

    fn handle_client(mut stream: TcpStream) {
        // let mut data = [0 as u8; 50]; // using 50 byte buffer
        let mut buffer = vec![0 as u8; 50];
        while match stream.read(&mut buffer) {
            Ok(size) => {
                // echo everything!
                // TODO: Drop the following loop for debugging
                for x in &buffer {
                    print!("{:02X?} ", x);
                }
                println!("\n");

                // First byte received is the message type.
                if buffer[0] != MsgType::FED_IDS.to_byte() {
                    if buffer[0] == MsgType::P2P_SENDING_FED_ID.to_byte()
                        || buffer[0] == MsgType::P2P_TAGGED_MESSAGE.to_byte()
                    {
                        // The federate is trying to connect to a peer, not to the RTI.
                        // It has connected to the RTI instead.
                        // FIXME: This should not happen, but apparently has been observed.
                        // It should not happen because the peers get the port and IP address
                        // of the peer they want to connect to from the RTI.
                        // If the connection is a peer-to-peer connection between two
                        // federates, reject the connection with the WRONG_SERVER error.
                        Self::send_reject(ErrType::WRONG_SERVER);
                    } else {
                        Self::send_reject(ErrType::UNEXPECTED_MESSAGE);
                    }
                    // TODO: Implement the following c code (tracing_enabled case)
                    // if (_f_rti->tracing_enabled){
                    //     tracepoint_rti_to_federate(_f_rti->trace, send_REJECT, fed_id, NULL);
                    // }
                    println!(
                        "RTI expected a MSG_TYPE_FED_IDS message. Got {} (see net_common.h).",
                        buffer[0]
                    );
                } else {
                    // Received federate ID.
                    let fed_id = u16::from_le_bytes(buffer[1..3].try_into().unwrap());
                    println!("RTI received federate ID: {}.", fed_id);

                    // Read the federation ID.  First read the length, which is one byte.
                    let federation_id_length = u8::from_le_bytes(buffer[3..4].try_into().unwrap());
                    println!("federationIdLength: {}", federation_id_length);
                    let last_index: usize = (4 + federation_id_length).into();

                    // TODO: Move federation_id to server struct.
                    let federation_id = "Undefined Federation1";

                    match String::from_utf8(buffer[4..last_index].to_vec()) {
                        Ok(federation_id_received) => {
                            println!("RTI received federation ID: {}.", federation_id_received);
                            // TODO: Implement the following c code (tracing_enabled case)
                            // if (_f_rti->tracing_enabled) {
                            //     tracepoint_rti_from_federate(_f_rti->trace, receive_FED_ID, fed_id, NULL);
                            // }
                            match federation_id_received == federation_id {
                                true => println!(
                                    "Federation ID matches! {} {}",
                                    federation_id, federation_id_received
                                ),
                                _ => {
                                    println!("ERROR: Federation ID does not match!");
                                    std::process::exit(1);
                                }
                            }

                            // TODO: Compare the received federation ID to mine.

                            // TODO: Assign the address information for federate.

                            // TODO: Set the federate's state as pending

                            println!("RTI responding with MSG_TYPE_ACK to federate {}.", fed_id);
                            // Send an MSG_TYPE_ACK message.
                            let mut ack_message: Vec<u8> = vec![MsgType::ACK.to_byte()];
                            // ack_message.push();
                            println!("MSG_TYPE_ACK message: {:?}", ack_message);
                            match stream.write(&ack_message) {
                                Ok(..) => {}
                                Err(e) => {
                                    println!(
                                        "RTI failed to write MSG_TYPE_ACK message to federate {}.",
                                        fed_id
                                    );
                                }
                            }

                            // (In Progress)
                            // TODO: Implement the following c code (tracing_enabled case)
                            // if (_f_rti->tracing_enabled) {
                            //     tracepoint_rti_to_federate(_f_rti->trace, send_ACK, fed_id, NULL);
                            // }
                        }
                        Err(e) => {
                            println!("Failed {}", e);
                        }
                    };
                }
                println!("");
                true
            }
            Err(_) => {
                println!(
                    "RTI failed to read federation id from federate.\nPeer address: {}",
                    stream.peer_addr().unwrap()
                );
                stream.shutdown(Shutdown::Both).unwrap();
                false
            }
        } {}
    }

    fn send_reject(err_msg: ErrType) {}
}
