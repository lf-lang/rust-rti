/**
 * @file
 * @author Hokeun Kim (hkim501@asu.edu)
 * @author Chanhee Lee (..)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::io::{Read, Write};
use std::mem;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

use crate::constants::*;
use crate::net_common::*;
use crate::Enclave;
use crate::FedState;
use crate::FedState::*;
use crate::Federate;
use crate::FederationRTI;

enum SocketType {
    TCP,
    UDP,
}

// impl SocketType {
//     pub fn to_inT(&self) -> i32 {
//         match self {
//             SocketType::TCP => 0,
//             SocketType::UDP => 1,
//         }
//     }
// }

pub struct Server {
    port: String,
}

impl Server {
    pub fn create_server(port: String) -> Server {
        // TODO: handle TCP and UDP cases
        Server { port }
    }

    pub fn wait_for_federates(&mut self, _f_rti: FederationRTI) {
        let mut address = String::from("0.0.0.0:");
        address.push_str(self.port.as_str());
        let socket = TcpListener::bind(address).unwrap();
        // accept connections and process them, spawning a new thread for each one
        println!("Server listening on port {}", self.port);
        let handles = self.connect_to_federates(socket, _f_rti);

        println!("RTI: All expected federates have connected. Starting execution.");

        for handle in handles {
            handle.join().unwrap();
        }

        // TODO: _f_rti.set_all_federates_exited(true);

        // The socket server will not continue to accept connections after all the federates
        // have joined.
        // In case some other federation's federates are trying to join the wrong
        // federation, need to respond. Start a separate thread to do that.
        // TODO: lf_thread_create(&responder_thread, respond_to_erroneous_connections, NULL);

        // Shutdown and close the socket so that the accept() call in
        // respond_to_erroneous_connections returns. That thread should then
        // check _f_rti->all_federates_exited and it should exit.
        // TODO: drop(socket);
        // NOTE: In all common TCP/IP stacks, there is a time period,
        // typically between 30 and 120 seconds, called the TIME_WAIT period,
        // before the port is released after this close. This is because
        // the OS is preventing another program from accidentally receiving
        // duplicated packets intended for this program.
        // TODO: close(socket_descriptor);
    }

    fn connect_to_federates(
        &mut self,
        socket: TcpListener,
        mut _f_rti: FederationRTI,
    ) -> Vec<JoinHandle<()>> {
        // TODO: Error-handling of unwrap()
        let number_of_enclaves: usize = _f_rti.number_of_enclaves().try_into().unwrap();
        let arc_rti = Arc::new(Mutex::new(_f_rti));
        let mut handle_list: Vec<JoinHandle<()>> = vec![];
        for _i in 0..number_of_enclaves {
            let cloned_rti = Arc::clone(&arc_rti);
            // Wait for an incoming connection request.
            // The following blocks until a federate connects.
            for stream in socket.incoming() {
                match stream {
                    Ok(mut stream) => {
                        println!("\nNew connection: {}", stream.peer_addr().unwrap());

                        // The first message from the federate should contain its ID and the federation ID.
                        let fed_id =
                            self.receive_and_check_fed_id_message(&mut stream, cloned_rti.clone());
                        // TODO: Error-handling of fed_id.try_into().unwrap()
                        if fed_id >= 0
                            && self.receive_connection_information(
                                fed_id.try_into().unwrap(),
                                &mut stream,
                                cloned_rti.clone(),
                            )
                        // TODO: Implement self.receive_udp_message_and_set_up_clock_sync()
                        {
                            // Create a thread to communicate with the federate.
                            // This has to be done after clock synchronization is finished
                            // or that thread may end up attempting to handle incoming clock
                            // synchronization messages.
                            let _handle = thread::spawn(move || {
                                // This closure is the implementation of federate_thread_TCP in rti_lib.c

                                // Buffer for incoming messages.
                                // This does not constrain the message size because messages
                                // are forwarded piece by piece.
                                let mut buffer = vec![0 as u8; FED_COM_BUFFER_SIZE];

                                // Listen for messages from the federate.
                                while true {
                                    {
                                        let mut locked_rti = cloned_rti.lock().unwrap();
                                        let fed: &mut Federate =
                                            &mut locked_rti.enclaves()[fed_id as usize];
                                        if fed.enclave().state() == FedState::NOT_CONNECTED {
                                            break;
                                        }
                                    }
                                    while match stream.read(&mut buffer) {
                                        Ok(bytes_read) => {
                                            let mut locked_rti = cloned_rti.lock().unwrap();
                                            let fed: &mut Federate =
                                                &mut locked_rti.enclaves()[fed_id as usize];

                                            if bytes_read < 1 {
                                                // Socket is closed

                                                println!("RTI: Socket to federate {} is closed. Exiting the thread.",
                                                    fed.enclave().id());
                                                fed.enclave().set_state(FedState::NOT_CONNECTED);
                                                // FIXME: We need better error handling here, but do not stop execution here.
                                            }
                                            println!(
                                                "RTI: Received message type {} from federate {}.",
                                                buffer[0],
                                                fed.enclave().id()
                                            );

                                            true
                                        }
                                        Err(_) => false,
                                    } {}
                                }
                            });
                            // TODO: Need to set handle to federate.thread_id?
                            handle_list.push(_handle);
                        }
                        break;
                    }
                    Err(e) => {
                        println!("RTI failed to accept the socket. {}.", e);
                        /* connection failed */
                        // FIXME: This should not exit on error, but rather just reject the connection.
                        std::process::exit(1);
                    }
                }
            }
        }

        handle_list
    }

    fn receive_and_check_fed_id_message(
        &mut self,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
    ) -> i32 {
        // Buffer for message ID, federate ID, and federation ID length.
        let length = 1 + mem::size_of::<u16>() + 1;
        let mut first_buffer = vec![0 as u8; length];
        let mut fed_id = u16::MAX;
        let cloned_rti = Arc::clone(&_f_rti);
        while match stream.read(&mut first_buffer) {
            Ok(size) => {
                // echo everything!
                // TODO: Drop the following loop for debugging
                print!("Message Size: {}\n", size);
                for x in &first_buffer {
                    print!("{:02X?} ", x);
                }
                println!("");

                // First byte received is the message type.
                if first_buffer[0] != MsgType::FED_IDS.to_byte() {
                    if first_buffer[0] == MsgType::P2P_SENDING_FED_ID.to_byte()
                        || first_buffer[0] == MsgType::P2P_TAGGED_MESSAGE.to_byte()
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
                    println!(
                        "RTI expected a MSG_TYPE_FED_IDS message. Got {} (see net_common.h).",
                        first_buffer[0]
                    );
                    return -1;
                } else {
                    // Received federate ID.
                    fed_id = u16::from_le_bytes(first_buffer[1..3].try_into().unwrap());
                    println!("RTI received federate ID: {}.", fed_id);

                    // Read the federation ID.  First read the length, which is one byte.
                    let federation_id_length =
                        u8::from_le_bytes(first_buffer[3..4].try_into().unwrap());
                    println!("federationIdLength: {}", federation_id_length);
                    let mut second_buffer = vec![0 as u8; federation_id_length.into()];
                    while match stream.read(&mut second_buffer) {
                        Ok(second_size) => {
                            match String::from_utf8(second_buffer.clone()) {
                                Ok(federation_id_received) => {
                                    println!(
                                        "RTI received federation ID: {}.",
                                        federation_id_received
                                    );

                                    let mut locked_rti = cloned_rti.lock().unwrap();
                                    // Compare the received federation ID to mine.
                                    if federation_id_received != locked_rti.federation_id() {
                                        // Federation IDs do not match. Send back a MSG_TYPE_REJECT message.
                                        println!(
                                            "WARNING: Federate from another federation {} attempted to connect to RTI in federation {}.",
                                            federation_id_received, locked_rti.federation_id()
                                        );
                                        Self::send_reject(ErrType::FEDERATION_ID_DOES_NOT_MATCH);
                                        std::process::exit(1);
                                    } else {
                                        if i32::from(fed_id) >= locked_rti.number_of_enclaves() {
                                            // Federate ID is out of range.
                                            println!(
                                                "RTI received federate ID {}, which is out of range.",
                                                fed_id
                                            );
                                            Self::send_reject(ErrType::FEDERATE_ID_OUT_OF_RANGE);
                                            std::process::exit(1);
                                        } else {
                                            let idx: usize = fed_id.into();
                                            let federate: &mut Federate =
                                                &mut locked_rti.enclaves()[idx];
                                            let enclave = federate.enclave();
                                            if enclave.state() != NOT_CONNECTED {
                                                println!(
                                                    "RTI received duplicate federate ID: {}.",
                                                    fed_id
                                                );
                                                Self::send_reject(ErrType::FEDERATE_ID_IN_USE);
                                                std::process::exit(1);
                                            }
                                        }
                                    }
                                    println!(
                                        "Federation ID matches! \"{}(received)\" <-> \"{}(_f_rti)\"",
                                        federation_id_received,
                                        locked_rti.federation_id()
                                    );

                                    // TODO: Assign the address information for federate.

                                    // Set the federate's state as pending
                                    // because it is waiting for the start time to be
                                    // sent by the RTI before beginning its execution.
                                    let idx: usize = fed_id.into();
                                    let federate: &mut Federate = &mut locked_rti.enclaves()[idx];
                                    let enclave: &mut Enclave = federate.enclave();
                                    enclave.set_state(FedState::PENDING);

                                    println!(
                                        "RTI responding with MSG_TYPE_ACK to federate {}.",
                                        fed_id
                                    );
                                    // Send an MSG_TYPE_ACK message.
                                    let mut ack_message: Vec<u8> = vec![MsgType::ACK.to_byte()];
                                    println!("MSG_TYPE_ACK message: {:?}", ack_message);
                                    match stream.write(&ack_message) {
                                        Ok(..) => {}
                                        Err(_e) => {
                                            println!(
                                                "RTI failed to write MSG_TYPE_ACK message to federate {}.",
                                                fed_id
                                            );
                                            // TODO: Handle errexit
                                        }
                                    }
                                    return fed_id.into();
                                }
                                Err(e) => {
                                    println!("Failed to convert a message buffer to a federation id ({})", e);
                                    return -1;
                                }
                            };
                        }
                        Err(_e) => {
                            println!("RTI failed to read federation id from federate {}.", fed_id);
                            return -1;
                        }
                    } {}
                }
                true
            }
            Err(_) => {
                // FIXME: This should not exit with error but rather should just reject the connection.
                println!(
                    "RTI failed to read from accepted socket.\nPeer address: {}",
                    stream.peer_addr().unwrap()
                );
                stream.shutdown(Shutdown::Both).unwrap();
                false
            }
        } {}

        fed_id.into()
    }

    fn receive_connection_information(
        &mut self,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
    ) -> bool {
        println!(
            "RTI waiting for MSG_TYPE_NEIGHBOR_STRUCTURE from federate {}.",
            fed_id
        );
        let cloned_rti = Arc::clone(&_f_rti);
        let mut locked_rti = cloned_rti.lock().unwrap();
        let mut connection_info_header =
            vec![0 as u8; MSG_TYPE_NEIGHBOR_STRUCTURE_HEADER_SIZE.try_into().unwrap()];
        while match stream.read(&mut connection_info_header) {
            Ok(first_recv_size) => {
                if connection_info_header[0] != MsgType::NEIGHBOR_STRUCTURE.to_byte() {
                    println!("RTI was expecting a MSG_TYPE_UDP_PORT message from federate {}. Got {} instead. Rejecting federate.", fed_id, connection_info_header[0]);
                    Self::send_reject(ErrType::UNEXPECTED_MESSAGE);
                    false
                } else {
                    let idx: usize = fed_id.into();
                    let federate: &mut Federate = &mut locked_rti.enclaves()[idx];
                    let enclave: &mut Enclave = federate.enclave();
                    enclave.set_num_upstream(connection_info_header[1].into());
                    enclave.set_num_downstream(
                        connection_info_header[1 + mem::size_of::<i32>()].into(),
                    );
                    println!(
                        "RTI got {} upstreams and {} downstreams from federate {}.",
                        enclave.num_upstream(),
                        enclave.num_downstream(),
                        fed_id
                    );

                    let num_upstream = enclave.num_upstream() as usize;
                    let num_downstream = enclave.num_downstream() as usize;
                    let connections_info_body_size =
                        ((mem::size_of::<u16>() + mem::size_of::<i64>()) * num_upstream)
                            + (mem::size_of::<u16>() * num_downstream);
                    let mut connection_info_body = vec![0 as u8; connections_info_body_size];
                    while match stream.read(&mut connection_info_body) {
                        Ok(second_recv_size) => {
                            // Keep track of where we are in the buffer
                            let mut message_head: usize = 0;
                            // First, read the info about upstream federates
                            for i in 0..num_upstream {
                                let upstream_id = u16::from_le_bytes(
                                    connection_info_body
                                        [message_head..(message_head + mem::size_of::<u16>())]
                                        .try_into()
                                        .unwrap(),
                                );
                                enclave.set_upstream_id_at(upstream_id, i);
                                message_head += mem::size_of::<u16>();
                                println!(
                                    "upstream_id: {}, message_head: {}",
                                    upstream_id, message_head
                                );
                                let upstream_delay = i64::from_le_bytes(
                                    connection_info_body
                                        [message_head..(message_head + mem::size_of::<i64>())]
                                        .try_into()
                                        .unwrap(),
                                );
                                enclave.set_upstream_delay_at(Some(upstream_delay), i);
                                message_head += mem::size_of::<i64>();
                                println!(
                                    "upstream_delay: {}, message_head: {}",
                                    upstream_delay, message_head
                                );
                            }

                            // Next, read the info about downstream federates
                            for i in 0..num_downstream {
                                let downstream_id = u16::from_le_bytes(
                                    connection_info_body
                                        [message_head..(message_head + mem::size_of::<u16>())]
                                        .try_into()
                                        .unwrap(),
                                );
                                enclave.set_downstream_id_at(downstream_id, i);
                                message_head += mem::size_of::<u16>();
                                println!(
                                    "downstream_id: {}, message_head: {}",
                                    downstream_id, message_head
                                );
                            }

                            false
                        }
                        Err(_) => {
                            println!("RTI failed to read MSG_TYPE_NEIGHBOR_STRUCTURE message body from federate {}.",
                            fed_id);
                            false
                        }
                    } {}

                    false
                }
            }
            Err(_) => {
                println!("RTI failed to read MSG_TYPE_NEIGHBOR_STRUCTURE message header from federate {}.",
                fed_id);
                stream.shutdown(Shutdown::Both).unwrap();
                false
            }
        } {}
        true
    }

    fn send_reject(_err_msg: ErrType) {}
}
