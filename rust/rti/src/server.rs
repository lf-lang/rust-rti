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
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;

use crate::constants::*;
use crate::message_record::message_record::MessageRecord;
use crate::net_common;
use crate::net_common::*;
use crate::net_util::*;
use crate::tag;
use crate::tag::*;
use crate::ClockSyncStat;
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
//     pub fn to_int(&self) -> i32 {
//         match self {
//             SocketType::TCP => 0,
//             SocketType::UDP => 1,
//         }
//     }
// }

struct StopGranted {
    _lf_rti_stop_granted_already_sent_to_federates: bool,
}

impl StopGranted {
    pub fn new() -> StopGranted {
        StopGranted {
            _lf_rti_stop_granted_already_sent_to_federates: false,
        }
    }

    pub fn _lf_rti_stop_granted_already_sent_to_federates(&self) -> bool {
        self._lf_rti_stop_granted_already_sent_to_federates
    }

    pub fn set_lf_rti_stop_granted_already_sent_to_federates(
        &mut self,
        _lf_rti_stop_granted_already_sent_to_federates: bool,
    ) {
        self._lf_rti_stop_granted_already_sent_to_federates =
            _lf_rti_stop_granted_already_sent_to_federates;
    }
}

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
        let mut start_time = Arc::new(Mutex::new(StartTime::new()));
        let received_start_times = Arc::new((Mutex::new(false), Condvar::new()));
        let sent_start_time = Arc::new((Mutex::new(false), Condvar::new()));
        let mut stop_granted = Arc::new(Mutex::new(StopGranted::new()));
        let handles = self.connect_to_federates(
            socket,
            _f_rti,
            start_time,
            received_start_times,
            sent_start_time,
            stop_granted,
        );

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
        start_time: Arc<Mutex<tag::StartTime>>,
        received_start_times: Arc<(Mutex<bool>, Condvar)>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
        mut stop_granted: Arc<Mutex<StopGranted>>,
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
                            && self.receive_udp_message_and_set_up_clock_sync(
                                fed_id.try_into().unwrap(),
                                &mut stream,
                                cloned_rti.clone(),
                            )
                        {
                            // Create a thread to communicate with the federate.
                            // This has to be done after clock synchronization is finished
                            // or that thread may end up attempting to handle incoming clock
                            // synchronization messages.
                            let mut cloned_start_time = Arc::clone(&start_time);
                            let mut cloned_received_start_times = Arc::clone(&received_start_times);
                            let mut cloned_sent_start_time = Arc::clone(&sent_start_time);
                            let mut cloned_stop_granted = Arc::clone(&stop_granted);
                            let _handle = thread::spawn(move || {
                                // This closure is the implementation of federate_thread_TCP in rti_lib.c
                                {
                                    let mut locked_rti = cloned_rti.lock().unwrap();
                                    let fed: &mut Federate =
                                        &mut locked_rti.enclaves()[fed_id as usize];
                                    fed.set_stream(stream.try_clone().unwrap());
                                }

                                // Buffer for incoming messages.
                                // This does not constrain the message size because messages
                                // are forwarded piece by piece.
                                let mut buffer = vec![0 as u8; FED_COM_BUFFER_SIZE];

                                // Listen for messages from the federate.
                                while true {
                                    // Read no more than one byte to get the message type.
                                    {
                                        let mut locked_rti = cloned_rti.lock().unwrap();
                                        let mut enclaves = locked_rti.enclaves();
                                        // FIXME:: Replace "as usize" properly.
                                        let fed: &mut Federate = &mut enclaves[fed_id as usize];
                                        println!("[connect_to_federates] FED_ID = {}", fed_id);
                                        let mut enclave = fed.enclave();
                                        if enclave.state() == FedState::NOT_CONNECTED {
                                            break;
                                        }
                                    }
                                    while match stream.read(&mut buffer) {
                                        Ok(bytes_read) => {
                                            {
                                                if bytes_read < 1 {
                                                    // Socket is closed
                                                    println!("RTI: Socket to federate {} is closed. Exiting the thread.",
                                                        fed_id);
                                                    // TODO: Enable the following line.
                                                    // fed.enclave()
                                                    //     .set_state(FedState::NOT_CONNECTED);
                                                    // FIXME: We need better error handling here, but do not stop execution here.
                                                    ()
                                                }
                                                println!(
                                                    "RTI: Received message type {} from federate {}.",
                                                    buffer[0],
                                                    fed_id
                                                );
                                            }

                                            false
                                        }
                                        Err(_) => false,
                                    } {}
                                    match MsgType::to_msg_type(buffer[0]) {
                                        MsgType::TIMESTAMP => Self::handle_timestamp(
                                            &buffer,
                                            fed_id.try_into().unwrap(),
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_received_start_times.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        MsgType::ADDRESS_QUERY => {
                                            Self::handle_address_query(cloned_rti.clone())
                                        }
                                        MsgType::TAGGED_MESSAGE => Self::handle_timed_message(
                                            &buffer,
                                            fed_id.try_into().unwrap(),
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                        ),
                                        MsgType::NEXT_EVENT_TAG => Self::handle_next_event_tag(
                                            &buffer,
                                            fed_id.try_into().unwrap(),
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        MsgType::LOGICAL_TAG_COMPLETE => {
                                            Self::handle_logical_tag_complete(
                                                &buffer,
                                                fed_id.try_into().unwrap(),
                                                cloned_rti.clone(),
                                                cloned_start_time.clone(),
                                                cloned_stop_granted.clone(),
                                                cloned_sent_start_time.clone(),
                                            )
                                        }
                                        MsgType::STOP_REQUEST => Self::handle_stop_request_message(
                                            &buffer,
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_stop_granted.clone(),
                                        ), // FIXME: Reviewed until here.
                                        // Need to also look at
                                        // notify_advance_grant_if_safe()
                                        // and notify_downstream_advance_grant_if_safe()
                                        _ => {
                                            let mut locked_rti = cloned_rti.lock().unwrap();
                                            let fed: &mut Federate =
                                                &mut locked_rti.enclaves()[fed_id as usize];
                                            println!("RTI received from federate {} an unrecognized TCP message type: {}.", fed.enclave().id(), buffer[0]);
                                        }
                                    }
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
        // All federates have connected.
        println!("All federates have connected to RTI.");

        let cloned_rti = Arc::clone(&arc_rti);
        let mut locked_rti = cloned_rti.lock().unwrap();
        let clock_sync_global_status = locked_rti.clock_sync_global_status();
        if clock_sync_global_status >= ClockSyncStat::CLOCK_SYNC_ON {
            // Create the thread that performs periodic PTP clock synchronization sessions
            // over the UDP channel, but only if the UDP channel is open and at least one
            // federate is performing runtime clock synchronization.
            let mut clock_sync_enabled = false;
            for i in 0..locked_rti.number_of_enclaves() {
                if locked_rti.enclaves()[i as usize].clock_synchronization_enabled() {
                    clock_sync_enabled = true;
                    break;
                }
            }
            if locked_rti.final_port_UDP() != u16::MAX && clock_sync_enabled {
                println!("\tNEED to create clock_synchronization_thread thread..");
                // TODO: Implement the following.
                // lf_thread_create(&_f_rti->clock_thread, clock_synchronization_thread, NULL);
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
                                    let mut number_of_enclaves = 0;
                                    let mut federation_id = String::new();
                                    {
                                        let mut locked_rti = cloned_rti.lock().unwrap();
                                        number_of_enclaves = locked_rti.number_of_enclaves();
                                        federation_id = locked_rti.federation_id();
                                    }
                                    // Compare the received federation ID to mine.
                                    if federation_id_received != federation_id {
                                        // Federation IDs do not match. Send back a MSG_TYPE_REJECT message.
                                        println!(
                                            "WARNING: Federate from another federation {} attempted to connect to RTI in federation {}.",
                                            federation_id_received, federation_id
                                        );
                                        Self::send_reject(ErrType::FEDERATION_ID_DOES_NOT_MATCH);
                                        std::process::exit(1);
                                    } else {
                                        if i32::from(fed_id) >= number_of_enclaves {
                                            // Federate ID is out of range.
                                            println!(
                                                "RTI received federate ID {}, which is out of range.",
                                                fed_id
                                            );
                                            Self::send_reject(ErrType::FEDERATE_ID_OUT_OF_RANGE);
                                            std::process::exit(1);
                                        } else {
                                            let mut locked_rti = cloned_rti.lock().unwrap();
                                            let idx: usize = fed_id.into();
                                            let federate: &mut Federate =
                                                &mut locked_rti.enclaves()[idx];
                                            let enclave = federate.enclave();
                                            if enclave.state() != FedState::NOT_CONNECTED {
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
                                        federation_id
                                    );

                                    // TODO: Assign the address information for federate.

                                    // Set the federate's state as pending
                                    // because it is waiting for the start time to be
                                    // sent by the RTI before beginning its execution.
                                    {
                                        let mut locked_rti = cloned_rti.lock().unwrap();
                                        let idx: usize = fed_id.into();
                                        let federate: &mut Federate =
                                            &mut locked_rti.enclaves()[idx];
                                        let enclave: &mut Enclave = federate.enclave();
                                        enclave.set_state(FedState::PENDING);
                                    }
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

    fn send_reject(_err_msg: ErrType) {}

    fn receive_connection_information(
        &mut self,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
    ) -> bool {
        println!(
            "RTI waiting for MsgType::NEIGHBOR_STRUCTURE from federate {}.",
            fed_id
        );
        let cloned_rti = Arc::clone(&_f_rti);
        let mut locked_rti = cloned_rti.lock().unwrap();
        let mut connection_info_header =
            vec![0 as u8; MSG_TYPE_NEIGHBOR_STRUCTURE_HEADER_SIZE.try_into().unwrap()];
        while match stream.read(&mut connection_info_header) {
            Ok(first_recv_size) => {
                if connection_info_header[0] != MsgType::NEIGHBOR_STRUCTURE.to_byte() {
                    println!("RTI was expecting a MsgType::NEIGHBOR_STRUCTURE message from federate {}. Got {} instead. Rejecting federate.", fed_id, connection_info_header[0]);
                    Self::send_reject(ErrType::UNEXPECTED_MESSAGE);
                    false
                } else {
                    let idx: usize = fed_id.into();
                    let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
                    let enclave: &mut Enclave = fed.enclave();
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
                                    "[{}] upstream_delay: {}, message_head: {}",
                                    i, upstream_delay, message_head
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
                            println!("RTI failed to read MsgType::NEIGHBOR_STRUCTURE message body from federate {}.",
                            fed_id);
                            false
                        }
                    } {}

                    false
                }
            }
            Err(_) => {
                println!("RTI failed to read MsgType::NEIGHBOR_STRUCTURE message header from federate {}.",
                fed_id);
                stream.shutdown(Shutdown::Both).unwrap();
                false
            }
        } {}
        true
    }

    fn receive_udp_message_and_set_up_clock_sync(
        &mut self,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
    ) -> bool {
        println!(
            "RTI waiting for MsgType::UDP_PORT from federate {}.",
            fed_id
        );
        let cloned_rti = Arc::clone(&_f_rti);
        let mut response = vec![0 as u8; 1 + mem::size_of::<u16>()];
        while match stream.read(&mut response) {
            Ok(first_recv_size) => {
                if response[0] != MsgType::UDP_PORT.to_byte() {
                    println!("RTI was expecting a MSG_TYPE_UDP_PORT message from federate {}. Got {} instead. Rejecting federate.", fed_id, response[0]);
                    Self::send_reject(ErrType::UNEXPECTED_MESSAGE);
                    return false;
                } else {
                    let mut clock_sync_global_status = ClockSyncStat::CLOCK_SYNC_INIT;
                    {
                        let mut locked_rti = cloned_rti.lock().unwrap();
                        clock_sync_global_status = locked_rti.clock_sync_global_status();
                    }

                    if clock_sync_global_status >= ClockSyncStat::CLOCK_SYNC_INIT {
                        // If no initial clock sync, no need perform initial clock sync.
                        let federate_UDP_port_number =
                            u16::from_le_bytes(response[1..3].try_into().unwrap());

                        println!(
                            "RTI got MsgType::UDP_PORT {} from federate {}.",
                            federate_UDP_port_number, fed_id
                        );
                        // A port number of UINT16_MAX means initial clock sync should not be performed.
                        if federate_UDP_port_number != u16::MAX {
                            // TODO: Implement this if body
                            println!(
                                "RTI finished initial clock synchronization with federate {}.",
                                fed_id
                            );
                        }
                        if clock_sync_global_status >= ClockSyncStat::CLOCK_SYNC_ON {
                            // If no runtime clock sync, no need to set up the UDP port.
                            if federate_UDP_port_number > 0 {
                                // Initialize the UDP_addr field of the federate struct
                                // TODO: Handle below assignments
                                // fed.UDP_addr.sin_family = AF_INET;
                                // fed.UDP_addr.sin_port = htons(federate_UDP_port_number);
                                // fed.UDP_addr.sin_addr = fed->server_ip_addr;
                            }
                        } else {
                            // Disable clock sync after initial round.
                            let mut locked_rti = cloned_rti.lock().unwrap();
                            let idx: usize = fed_id.into();
                            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
                            fed.set_clock_synchronization_enabled(false);
                        }
                    } else {
                        // No clock synchronization at all.
                        // Clock synchronization is universally disabled via the clock-sync command-line parameter
                        // (-c off was passed to the RTI).
                        // Note that the federates are still going to send a MSG_TYPE_UDP_PORT message but with a payload (port) of -1.
                        let mut locked_rti = cloned_rti.lock().unwrap();
                        let idx: usize = fed_id.into();
                        let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
                        fed.set_clock_synchronization_enabled(false);
                    }
                }

                false
            }
            Err(e) => {
                println!(
                    "RTI failed to read MsgType::UDP_PORT message from federate {}.",
                    fed_id
                );
                false
            }
        } {}

        true
    }

    fn handle_timestamp(
        buffer: &Vec<u8>,
        fed_id: u16,
        // stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        received_start_times: Arc<(Mutex<bool>, Condvar)>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let buffer_size = mem::size_of::<i64>();
        // FIXME: Check whether swap_bytes_if_big_endian_int64() is implemented correctly
        let timestamp = i64::from_le_bytes(buffer[1..(1 + buffer_size)].try_into().unwrap());
        println!("RTI received timestamp message with time: {} .", timestamp);

        let mut num_feds_proposed_start = 0;
        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.number_of_enclaves();
            let max_start_time = locked_rti.max_start_time();
            num_feds_proposed_start = locked_rti.num_feds_proposed_start();
            num_feds_proposed_start += 1;
            locked_rti.set_num_feds_proposed_start(num_feds_proposed_start);
            if timestamp > max_start_time {
                locked_rti.set_max_start_time(timestamp);
            }
        }
        if num_feds_proposed_start == number_of_enclaves {
            // All federates have proposed a start time.
            let received_start_times_notifier = Arc::clone(&received_start_times);
            let (lock, condvar) = &*received_start_times_notifier;
            let mut notified = lock.lock().unwrap();
            *notified = true;
            condvar.notify_all();
        } else {
            // Some federates have not yet proposed a start time.
            // wait for a notification.
            while num_feds_proposed_start < number_of_enclaves {
                // FIXME: Should have a timeout here?
                let (lock, condvar) = &*received_start_times;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
                {
                    let mut locked_rti = _f_rti.lock().unwrap();
                    num_feds_proposed_start = locked_rti.num_feds_proposed_start();
                }
            }
        }

        // Send back to the federate the maximum time plus an offset on a TIMESTAMP
        // message.
        let mut start_time_buffer = vec![0 as u8; MSG_TYPE_TIMESTAMP_LENGTH];
        start_time_buffer[0] = MsgType::TIMESTAMP.to_byte();
        // Add an offset to this start time to get everyone starting together.
        let mut max_start_time = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            max_start_time = locked_rti.max_start_time();
        }
        let mut locked_start_time = start_time.lock().unwrap();
        locked_start_time.set_start_time(max_start_time + net_common::DELAY_START);
        // TODO: Consider swap_bytes_if_big_endian_int64()
        NetUtil::encode_int64(locked_start_time.start_time(), &mut start_time_buffer, 1);

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            let mut stream = my_fed.stream().as_ref().unwrap();
            match stream.write(&start_time_buffer) {
                Ok(..) => {}
                Err(_e) => {
                    println!(
                        "Failed to send the starting time to federate {}.",
                        my_fed.enclave().id()
                    );
                    // TODO: Handle errexit
                }
            }

            // Update state for the federate to indicate that the MSG_TYPE_TIMESTAMP
            // message has been sent. That MSG_TYPE_TIMESTAMP message grants time advance to
            // the federate to the start time.
            my_fed.enclave().set_state(FedState::GRANTED);
            let sent_start_time_notifier = Arc::clone(&sent_start_time);
            let (lock, condvar) = &*sent_start_time_notifier;
            let mut notified = lock.lock().unwrap();
            *notified = true;
            condvar.notify_all();
            println!(
                "RTI sent start time {} to federate {}.",
                locked_start_time.start_time(),
                my_fed.enclave().id()
            );
        }
    }

    fn handle_address_query(_f_rti: Arc<Mutex<FederationRTI>>) {}

    fn handle_timed_message(
        buffer: &Vec<u8>,
        fed_id: u16,
        // stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
        start_time: Arc<Mutex<tag::StartTime>>,
    ) {
    }

    fn handle_next_event_tag(
        buffer: &Vec<u8>,
        fed_id: u16,
        // stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        // for x in buffer {
        //     print!("{:02X?} ", x);
        // }
        // println!("\n");

        // Acquire a mutex lock to ensure that this state does not change while a
        // message is in transport or being used to determine a TAG.
        let mut enclave_id = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            enclave_id = fed.enclave().id();
        }
        let intended_tag = NetUtil::extract_tag(
            buffer[1..(1 + mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        let mut start_time_value = 0;
        {
            let mut locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        println!(
            "RTI received from federate {} the Next Event Tag (NET) ({},{})",
            enclave_id,
            intended_tag.time() - start_time_value,
            intended_tag.microstep()
        );
        Self::update_federate_next_event_tag_locked(
            _f_rti,
            fed_id,
            intended_tag,
            start_time_value,
            sent_start_time,
        );
    }

    fn update_federate_next_event_tag_locked(
        _f_rti: Arc<Mutex<FederationRTI>>,
        fed_id: u16,
        mut next_event_tag: Tag,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut min_in_transit_tag = Tag::new(0, 0);
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            min_in_transit_tag = MessageRecord::get_minimum_in_transit_message_tag(
                fed.in_transit_message_tags(),
                start_time,
            );
        }
        if Tag::lf_tag_compare(&min_in_transit_tag, &next_event_tag) < 0 {
            next_event_tag = min_in_transit_tag.clone();
        }
        Enclave::update_enclave_next_event_tag_locked(
            _f_rti,
            fed_id,
            next_event_tag,
            start_time,
            sent_start_time,
        );
    }

    fn handle_logical_tag_complete(
        buffer: &Vec<u8>,
        fed_id: u16,
        // stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        stop_granted: Arc<Mutex<StopGranted>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let completed = NetUtil::extract_tag(
            buffer[1..(1 + mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.number_of_enclaves();
        }
        let mut start_time_value = 0;
        {
            let mut locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Enclave::logical_tag_complete(
            _f_rti.clone(),
            fed_id,
            number_of_enclaves,
            start_time_value,
            sent_start_time,
            completed.clone(),
        );

        // See if we can remove any of the recorded in-transit messages for this.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            let in_transit_message_tags = fed.in_transit_message_tags();
            MessageRecord::clean_in_transit_message_record_up_to_tag(
                in_transit_message_tags,
                completed,
                start_time_value,
            );
        }
    }

    fn handle_stop_request_message(
        buffer: &Vec<u8>,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<FederationRTI>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        stop_granted: Arc<Mutex<StopGranted>>,
    ) {
        println!("RTI handling stop_request from federate {}.", fed_id);

        // Acquire a mutex lock to ensure that this state does change while a
        // message is in transport or being used to determine a TAG.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];

            // Check whether we have already received a stop_tag
            // from this federate
            if fed.requested_stop() {
                // Ignore this request
                return;
            }
        }

        // Extract the proposed stop tag for the federate
        let proposed_stop_tag = NetUtil::extract_tag(
            buffer[1..(1 + mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        // Update the maximum stop tag received from federates
        let mut start_time_value = 0;
        {
            let mut locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            if Tag::lf_tag_compare(&proposed_stop_tag, &locked_rti.max_stop_tag()) > 0 {
                locked_rti.set_max_stop_tag(proposed_stop_tag.clone());
            }
        }

        println!(
            "RTI received from federate {} a MsgType::STOP_REQUEST message with tag ({},{}).",
            fed_id,
            proposed_stop_tag.time() - start_time_value,
            proposed_stop_tag.microstep()
        );

        // If this federate has not already asked
        // for a stop, add it to the tally.
        Self::mark_federate_requesting_stop(
            fed_id,
            _f_rti.clone(),
            stop_granted.clone(),
            start_time_value,
        );

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            if locked_rti.num_enclaves_handling_stop() == locked_rti.number_of_enclaves() {
                // We now have information about the stop time of all
                // federates. This is extremely unlikely, but it can occur
                // all federates call lf_request_stop() at the same tag.
                return;
            }
        }
        // Forward the stop request to all other federates that have not
        // also issued a stop request.
        let mut stop_request_buffer = vec![0 as u8; MSG_TYPE_STOP_REQUEST_LENGTH];
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            Self::encode_stop_request(
                &mut stop_request_buffer,
                locked_rti.max_stop_tag().time(),
                locked_rti.max_stop_tag().microstep(),
            );
        }

        // Iterate over federates and send each the MSG_TYPE_STOP_REQUEST message
        // if we do not have a stop_time already for them. Do not do this more than once.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            if locked_rti.stop_in_progress() {
                return;
            }
            locked_rti.set_stop_in_progress(true);
        }
        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.number_of_enclaves();
        }
        for i in 0..number_of_enclaves {
            let mut locked_rti = _f_rti.lock().unwrap();
            // FIXME: Handle usize properly.
            let f: &mut Federate = &mut locked_rti.enclaves()[i as usize];
            if f.e().id() != fed_id && f.requested_stop() == false {
                if f.e().state() == FedState::NOT_CONNECTED {
                    Self::mark_federate_requesting_stop(
                        f.e().id(),
                        _f_rti.clone(),
                        stop_granted.clone(),
                        start_time_value,
                    );
                    continue;
                }
                let mut stream = f.stream().as_ref().unwrap();
                match stream.write(&stop_request_buffer) {
                    Ok(..) => {}
                    Err(_e) => {
                        println!(
                            "RTI failed to forward MsgType::STOP_REQUEST message to federate {}.",
                            f.e().id()
                        );
                        // TODO: Handle errexit
                    }
                }
            }
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            println!(
                "RTI forwarded to federates MsgType::STOP_REQUEST with tag ({}, {}).",
                locked_rti.max_stop_tag().time() - start_time_value,
                locked_rti.max_stop_tag().microstep()
            );
        }
    }

    fn mark_federate_requesting_stop(
        fed_id: u16,
        _f_rti: Arc<Mutex<FederationRTI>>,
        stop_granted: Arc<Mutex<StopGranted>>,
        start_time_value: Instant,
    ) {
        let mut num_enclaves_handling_stop = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            num_enclaves_handling_stop = locked_rti.num_enclaves_handling_stop();
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            if !fed.requested_stop() {
                // Assume that the federate
                // has requested stop
                locked_rti.set_num_enclaves_handling_stop(num_enclaves_handling_stop + 1);
            }
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            if !fed.requested_stop() {
                // Assume that the federate
                // has requested stop
                fed.set_requested_stop(true);
            }
        }
        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            num_enclaves_handling_stop = locked_rti.num_enclaves_handling_stop();
            number_of_enclaves = locked_rti.number_of_enclaves();
        }
        if num_enclaves_handling_stop == number_of_enclaves {
            // We now have information about the stop time of all
            // federates.
            Self::_lf_rti_broadcast_stop_time_to_federates_locked(
                _f_rti,
                stop_granted,
                start_time_value,
            );
        }
    }

    /**
     * Once the RTI has seen proposed tags from all connected federates,
     * it will broadcast a MSG_TYPE_STOP_GRANTED carrying the _RTI.max_stop_tag.
     * This function also checks the most recently received NET from
     * each federate and resets that be no greater than the _RTI.max_stop_tag.
     *
     * This function assumes the caller holds the _RTI.rti_mutex lock.
     */
    fn _lf_rti_broadcast_stop_time_to_federates_locked(
        _f_rti: Arc<Mutex<FederationRTI>>,
        stop_granted: Arc<Mutex<StopGranted>>,
        start_time_value: Instant,
    ) {
        {
            let mut _stop_granted = stop_granted.lock().unwrap();
            if _stop_granted._lf_rti_stop_granted_already_sent_to_federates() == true {
                return;
            }
        }
        // Reply with a stop granted to all federates
        let mut outgoing_buffer = vec![0 as u8; MSG_TYPE_STOP_GRANTED_LENGTH];
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            Self::encode_stop_granted(
                &mut outgoing_buffer,
                locked_rti.max_stop_tag().time(),
                locked_rti.max_stop_tag().microstep(),
            );
        }

        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.number_of_enclaves();
        }
        // Iterate over federates and send each the message.
        for i in 0..number_of_enclaves {
            let mut next_event = Tag::never_tag();
            let mut max_stop_tag = Tag::never_tag();
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                max_stop_tag = locked_rti.max_stop_tag();
                // FIXME: Handle usize properly.
                let mut fed: &Federate = &locked_rti.enclaves()[i as usize];
                next_event = fed.e().next_event();
                if fed.e().state() == FedState::NOT_CONNECTED {
                    continue;
                }
            }
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                // FIXME: Handle usize properly.
                let mut fed: &mut Federate = &mut locked_rti.enclaves()[i as usize];
                if Tag::lf_tag_compare(&next_event, &max_stop_tag) >= 0 {
                    // Need the next_event to be no greater than the stop tag.
                    fed.enclave().set_next_event(max_stop_tag);
                }
            }
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                // FIXME: Handle usize properly.
                let mut fed: &mut Federate = &mut locked_rti.enclaves()[i as usize];
                let mut stream = fed.stream().as_ref().unwrap();
                match stream.write(&outgoing_buffer) {
                    Ok(..) => {}
                    Err(_e) => {
                        println!(
                            "RTI failed to forward MsgType::STOP_GRANTED message to federate {}.",
                            fed.e().id()
                        );
                        // TODO: Handle errexit
                    }
                }
            }
        }

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            println!(
                "RTI sent to federates MsgType::STOP_GRANTED with tag ({}, {}).",
                locked_rti.max_stop_tag().time() - start_time_value,
                locked_rti.max_stop_tag().microstep()
            );
        }
        {
            let mut _stop_granted = stop_granted.lock().unwrap();
            _stop_granted.set_lf_rti_stop_granted_already_sent_to_federates(true);
        }
    }

    // FIXME: Replace this function to a macro if needed.
    fn encode_stop_granted(outgoing_buffer: &mut Vec<u8>, time: Instant, microstep: Microstep) {
        outgoing_buffer[0] = MsgType::STOP_GRANTED.to_byte();
        NetUtil::encode_int64(time, outgoing_buffer, 1);
        assert!(microstep >= 0);
        NetUtil::encode_int32(
            microstep as i32,
            outgoing_buffer,
            1 + std::mem::size_of::<Instant>(),
        );
    }

    // FIXME: Replace this function to a macro if needed.
    fn encode_stop_request(stop_request_buffer: &mut Vec<u8>, time: Instant, microstep: Microstep) {
        stop_request_buffer[0] = MsgType::STOP_REQUEST.to_byte();
        NetUtil::encode_int64(time, stop_request_buffer, 1);
        assert!(microstep >= 0);
        NetUtil::encode_int32(
            microstep as i32,
            stop_request_buffer,
            1 + std::mem::size_of::<Instant>(),
        );
    }
}
