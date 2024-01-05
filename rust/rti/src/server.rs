/**
 * @file
 * @author Hokeun Kim (hokeun@asu.edu)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::io::Write;
use std::mem;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;

use crate::message_record::message_record::MessageRecord;
use crate::net_common;
use crate::net_common::*;
use crate::net_util::*;
use crate::tag;
use crate::tag::*;
use crate::ClockSyncStat;
use crate::FederateInfo;
use crate::RTIRemote;
use crate::SchedulingNode;
use crate::SchedulingNodeState;

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

    pub fn wait_for_federates(&mut self, _f_rti: RTIRemote) {
        println!("Server listening on port {}", self.port);
        let mut address = String::from("0.0.0.0:");
        address.push_str(self.port.as_str());
        let socket = TcpListener::bind(address).unwrap();
        let start_time = Arc::new(Mutex::new(StartTime::new()));
        let received_start_times = Arc::new((Mutex::new(false), Condvar::new()));
        let sent_start_time = Arc::new((Mutex::new(false), Condvar::new()));
        let stop_granted = Arc::new(Mutex::new(StopGranted::new()));
        // Wait for connections from federates and create a thread for each.
        let handles = self.connect_to_federates(
            socket,
            _f_rti,
            start_time,
            received_start_times,
            sent_start_time,
            stop_granted,
        );

        // All federates have connected.
        println!("RTI: All expected federates have connected. Starting execution.");

        // The socket server will not continue to accept connections after all the federates
        // have joined.
        // In case some other federation's federates are trying to join the wrong
        // federation, need to respond. Start a separate thread to do that.
        // TODO: lf_thread_create(&responder_thread, respond_to_erroneous_connections, NULL);

        // Wait for federate_info threads to exit.
        for handle in handles {
            handle.join().unwrap();
        }

        // TODO: _f_rti.set_all_federates_exited(true);

        // Shutdown and close the socket so that the accept() call in
        // respond_to_erroneous_connections returns. That thread should then
        // check _f_rti->all_federates_exited and it should exit.
        // TODO: shutdown(socket);

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
        mut _f_rti: RTIRemote,
        start_time: Arc<Mutex<tag::StartTime>>,
        received_start_times: Arc<(Mutex<bool>, Condvar)>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
        stop_granted: Arc<Mutex<StopGranted>>,
    ) -> Vec<JoinHandle<()>> {
        // TODO: Error-handling of unwrap()
        let number_of_enclaves: usize = _f_rti
            .base()
            .number_of_scheduling_nodes()
            .try_into()
            .unwrap();
        let arc_rti = Arc::new(Mutex::new(_f_rti));
        let mut handle_list: Vec<JoinHandle<()>> = vec![];
        for _i in 0..number_of_enclaves {
            let cloned_rti = Arc::clone(&arc_rti);
            // Wait for an incoming connection request.
            // The following blocks until a federate_info connects.
            for stream in socket.incoming() {
                match stream {
                    Ok(mut stream) => {
                        println!("\nNew connection: {}", stream.peer_addr().unwrap());

                        // The first message from the federate_info should contain its ID and the federation ID.
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
                            // Create a thread to communicate with the federate_info.
                            // This has to be done after clock synchronization is finished
                            // or that thread may end up attempting to handle incoming clock
                            // synchronization messages.
                            let cloned_start_time = Arc::clone(&start_time);
                            let cloned_received_start_times = Arc::clone(&received_start_times);
                            let cloned_sent_start_time = Arc::clone(&sent_start_time);
                            let cloned_stop_granted = Arc::clone(&stop_granted);
                            let _handle = thread::spawn(move || {
                                // This closure is the implementation of federate_thread_TCP in rti_lib.c
                                {
                                    let mut locked_rti = cloned_rti.lock().unwrap();
                                    let fed: &mut FederateInfo =
                                        &mut locked_rti.base().scheduling_nodes()[fed_id as usize];
                                    fed.set_stream(stream.try_clone().unwrap());
                                }

                                // Buffer for incoming messages.
                                // This does not constrain the message size because messages
                                // are forwarded piece by piece.
                                let mut buffer = vec![0 as u8; 1];

                                // Listen for messages from the federate_info.
                                loop {
                                    {
                                        let mut locked_rti = cloned_rti.lock().unwrap();
                                        let enclaves = locked_rti.base().scheduling_nodes();
                                        let fed: &mut FederateInfo = &mut enclaves[fed_id as usize];
                                        let enclave = fed.enclave();
                                        if enclave.state() == SchedulingNodeState::NotConnected {
                                            break;
                                        }
                                    }
                                    // Read no more than one byte to get the message type.
                                    // FIXME: Handle unwrap properly.
                                    let bytes_read = NetUtil::read_from_stream(
                                        &mut stream,
                                        &mut buffer,
                                        fed_id.try_into().unwrap(),
                                    );
                                    if bytes_read < 1 {
                                        // Socket is closed
                                        println!("RTI: Socket to federate_info {} is closed. Exiting the thread.",
                                            fed_id);
                                        let mut locked_rti = cloned_rti.lock().unwrap();
                                        let enclaves = locked_rti.base().scheduling_nodes();
                                        let fed: &mut FederateInfo = &mut enclaves[fed_id as usize];
                                        fed.enclave().set_state(SchedulingNodeState::NotConnected);
                                        // FIXME: We need better error handling here, but do not stop execution here.
                                        break;
                                    }
                                    println!(
                                        "RTI: Received message type {} from federate_info {}.",
                                        buffer[0], fed_id
                                    );
                                    match MsgType::to_msg_type(buffer[0]) {
                                        MsgType::Timestamp => Self::handle_timestamp(
                                            // &buffer,
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_received_start_times.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        MsgType::Resign => {
                                            Self::handle_federate_resign(
                                                fed_id.try_into().unwrap(),
                                                cloned_rti.clone(),
                                                cloned_start_time.clone(),
                                                cloned_sent_start_time.clone(),
                                            );
                                            return;
                                        }
                                        MsgType::TaggedMessage => Self::handle_timed_message(
                                            buffer[0],
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        MsgType::NextEventTag => Self::handle_next_event_tag(
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        MsgType::LogicalTagComplete => {
                                            Self::handle_logical_tag_complete(
                                                fed_id.try_into().unwrap(),
                                                &mut stream,
                                                cloned_rti.clone(),
                                                cloned_start_time.clone(),
                                                cloned_sent_start_time.clone(),
                                            )
                                        }
                                        // FIXME: Reviewed until here.
                                        // Need to also look at
                                        // notify_advance_grant_if_safe()
                                        // and notify_downstream_advance_grant_if_safe()
                                        MsgType::StopRequest => Self::handle_stop_request_message(
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_stop_granted.clone(),
                                        ),
                                        MsgType::StopRequestReply => {
                                            Self::handle_stop_request_reply(
                                                fed_id.try_into().unwrap(),
                                                &mut stream,
                                                cloned_rti.clone(),
                                                cloned_start_time.clone(),
                                                cloned_stop_granted.clone(),
                                            )
                                        }
                                        MsgType::PortAbsent => Self::handle_port_absent_message(
                                            &buffer,
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        _ => {
                                            let mut locked_rti = cloned_rti.lock().unwrap();
                                            let fed: &mut FederateInfo =
                                                &mut locked_rti.base().scheduling_nodes()
                                                    [fed_id as usize];
                                            println!("RTI received from federate_info {} an unrecognized TCP message type: {}.", fed.enclave().id(), buffer[0]);
                                        }
                                    }
                                }
                            });
                            // TODO: Need to set handle to federate_info.thread_id?
                            handle_list.push(_handle);
                        }
                        break;
                    }
                    Err(e) => {
                        // Try again
                        println!("RTI failed to accept the socket. {}.", e);
                    }
                }
            }
        }
        // All federates have connected.
        println!("All federates have connected to RTI.");

        let cloned_rti = Arc::clone(&arc_rti);
        let mut locked_rti = cloned_rti.lock().unwrap();
        let clock_sync_global_status = locked_rti.clock_sync_global_status();
        if clock_sync_global_status >= ClockSyncStat::ClockSyncOn {
            // Create the thread that performs periodic PTP clock synchronization sessions
            // over the UDP channel, but only if the UDP channel is open and at least one
            // federate_info is performing runtime clock synchronization.
            let mut clock_sync_enabled = false;
            for i in 0..locked_rti.base().number_of_scheduling_nodes() {
                if locked_rti.base().scheduling_nodes()[i as usize].clock_synchronization_enabled()
                {
                    clock_sync_enabled = true;
                    break;
                }
            }
            if locked_rti.final_port_udp() != u16::MAX && clock_sync_enabled {
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
        _f_rti: Arc<Mutex<RTIRemote>>,
    ) -> i32 {
        // Buffer for message ID, federate_info ID, and federation ID length.
        let length = 1 + mem::size_of::<u16>() + 1;
        let mut first_buffer = vec![0 as u8; length];

        // Read bytes from the socket. We need 4 bytes.
        // FIXME: This should not exit with error but rather should just reject the connection.
        NetUtil::read_from_stream_errexit(stream, &mut first_buffer, 0, "");

        // Initialize to an invalid value.
        let fed_id;

        // First byte received is the message type.
        if first_buffer[0] != MsgType::FedIds.to_byte() {
            if first_buffer[0] == MsgType::P2pSendingFedId.to_byte()
                || first_buffer[0] == MsgType::P2pTaggedMessage.to_byte()
            {
                // The federate_info is trying to connect to a peer, not to the RTI.
                // It has connected to the RTI instead.
                // FIXME: This should not happen, but apparently has been observed.
                // It should not happen because the peers get the port and IP address
                // of the peer they want to connect to from the RTI.
                // If the connection is a peer-to-peer connection between two
                // federates, reject the connection with the WrongServer error.
                Self::send_reject(stream, ErrType::WrongServer.to_byte());
            } else {
                Self::send_reject(stream, ErrType::UnexpectedMessage.to_byte());
            }
            println!(
                "RTI expected a MsgType::FedIds message. Got {} (see net_common.h).",
                first_buffer[0]
            );
            return -1;
        } else {
            // Received federate_info ID.
            // FIXME: Change from_le_bytes properly.
            let u16_size = mem::size_of::<u16>();
            fed_id = u16::from_le_bytes(first_buffer[1..(1 + u16_size)].try_into().unwrap());
            println!("RTI received federate_info ID: {}.", fed_id);

            // Read the federation ID.  First read the length, which is one byte.
            // FIXME: Change from_le_bytes properly.
            let federation_id_length = u8::from_le_bytes(
                first_buffer[(1 + u16_size)..(1 + u16_size + 1)]
                    .try_into()
                    .unwrap(),
            );
            let mut federation_id_buffer = vec![0 as u8; federation_id_length.into()];
            // Next read the actual federation ID.
            // FIXME: This should not exit on error, but rather just reject the connection.
            NetUtil::read_from_stream_errexit(
                stream,
                &mut federation_id_buffer,
                fed_id,
                "federation id",
            );
            let federation_id_received;
            match String::from_utf8(federation_id_buffer) {
                Ok(federation_id) => {
                    federation_id_received = federation_id;
                }
                Err(e) => {
                    println!(
                        "Failed to convert a message buffer to a federation id ({})",
                        e
                    );
                    return -1;
                }
            }

            println!("RTI received federation ID: {}.", federation_id_received);
            let cloned_rti = Arc::clone(&_f_rti);
            let number_of_enclaves;
            let federation_id;
            {
                let mut locked_rti = cloned_rti.lock().unwrap();
                number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
                federation_id = locked_rti.federation_id();
            }
            // Compare the received federation ID to mine.
            if federation_id_received != federation_id {
                // Federation IDs do not match. Send back a MSG_TYPE_Reject message.
                println!(
                    "WARNING: FederateInfo from another federation {} attempted to connect to RTI in federation {}.",
                    federation_id_received, federation_id
                );
                Self::send_reject(stream, ErrType::FederationIdDoesNotMatch.to_byte());
                return -1;
            } else {
                if i32::from(fed_id) >= number_of_enclaves {
                    // FederateInfo ID is out of range.
                    println!(
                        "RTI received federate_info ID {}, which is out of range.",
                        fed_id
                    );
                    Self::send_reject(stream, ErrType::FederateIdOutOfRange.to_byte());
                    return -1;
                } else {
                    let mut locked_rti = cloned_rti.lock().unwrap();
                    let idx: usize = fed_id.into();
                    let federate_info: &mut FederateInfo =
                        &mut locked_rti.base().scheduling_nodes()[idx];
                    let enclave = federate_info.enclave();
                    if enclave.state() != SchedulingNodeState::NotConnected {
                        println!("RTI received duplicate federate_info ID: {}.", fed_id);
                        Self::send_reject(stream, ErrType::FederateIdInUse.to_byte());
                        return -1;
                    }
                }
            }
            println!(
                "Federation ID matches! \"{}(received)\" <-> \"{}(_f_rti)\"",
                federation_id_received, federation_id
            );

            // TODO: Assign the address information for federate_info.

            // Set the federate_info's state as pending
            // because it is waiting for the start time to be
            // sent by the RTI before beginning its execution.
            {
                let mut locked_rti = cloned_rti.lock().unwrap();
                let idx: usize = fed_id.into();
                let federate_info: &mut FederateInfo =
                    &mut locked_rti.base().scheduling_nodes()[idx];
                let enclave: &mut SchedulingNode = federate_info.enclave();
                enclave.set_state(SchedulingNodeState::Pending);
            }
            println!(
                "RTI responding with MsgType::Ack to federate_info {}.",
                fed_id
            );
            // Send an MsgType::Ack message.
            let ack_message: Vec<u8> = vec![MsgType::Ack.to_byte()];
            NetUtil::write_to_stream_errexit(stream, &ack_message, fed_id, "MsgType::Ack message");
        }

        fed_id.into()
    }

    fn send_reject(stream: &mut TcpStream, error_code: u8) {
        println!("RTI sending MsgType::Reject.");
        let mut response = vec![0 as u8; 2];
        response[0] = MsgType::Reject.to_byte();
        response[1] = error_code;
        // NOTE: Ignore errors on this response.
        match stream.write(&response) {
            Ok(..) => {}
            Err(_e) => {
                println!("RTI failed to write MsgType::Reject message on the stream.");
                // TODO: Handle errexit
                std::process::exit(1);
            }
        }
        // Close the socket.
        stream
            .shutdown(Shutdown::Both)
            .expect("shutdown call failed");
    }

    fn receive_connection_information(
        &mut self,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
    ) -> bool {
        println!(
            "RTI waiting for MsgType::NeighborStructure from federate_info {}.",
            fed_id
        );
        let cloned_rti = Arc::clone(&_f_rti);
        let mut locked_rti = cloned_rti.lock().unwrap();
        let mut connection_info_header =
            vec![0 as u8; MSG_TYPE_NEIGHBOR_STRUCTURE_HEADER_SIZE.try_into().unwrap()];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut connection_info_header,
            fed_id,
            "MsgType::NeighborStructure message header",
        );

        if connection_info_header[0] != MsgType::NeighborStructure.to_byte() {
            println!("RTI was expecting a MsgType::NeighborStructure message from federate_info {}. Got {} instead. Rejecting federate_info.", fed_id, connection_info_header[0]);
            Self::send_reject(stream, ErrType::UnexpectedMessage.to_byte());
            return false;
        } else {
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            let enclave: &mut SchedulingNode = fed.enclave();
            // Read the number of upstream and downstream connections
            enclave.set_num_upstream(connection_info_header[1].into());
            enclave.set_num_downstream(connection_info_header[1 + mem::size_of::<i32>()].into());
            println!(
                "RTI got {} upstreams and {} downstreams from federate_info {}.",
                enclave.num_upstream(),
                enclave.num_downstream(),
                fed_id
            );

            let num_upstream = enclave.num_upstream() as usize;
            let num_downstream = enclave.num_downstream() as usize;
            let connections_info_body_size = ((mem::size_of::<u16>() + mem::size_of::<i64>())
                * num_upstream)
                + (mem::size_of::<u16>() * num_downstream);
            let mut connection_info_body = vec![0 as u8; connections_info_body_size];
            NetUtil::read_from_stream_errexit(
                stream,
                &mut connection_info_body,
                fed_id,
                "MsgType::NeighborStructure message body",
            );

            // Keep track of where we are in the buffer
            let mut message_head: usize = 0;
            // First, read the info about upstream federates
            for i in 0..num_upstream {
                // FIXME: Change from_le_bytes properly.
                let upstream_id = u16::from_le_bytes(
                    connection_info_body[message_head..(message_head + mem::size_of::<u16>())]
                        .try_into()
                        .unwrap(),
                );
                enclave.set_upstream_id_at(upstream_id, i);
                message_head += mem::size_of::<u16>();
                println!(
                    "upstream_id: {}, message_head: {}",
                    upstream_id, message_head
                );
                // FIXME: Change from_le_bytes properly.
                let upstream_delay = i64::from_le_bytes(
                    connection_info_body[message_head..(message_head + mem::size_of::<i64>())]
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
                // FIXME: Change from_le_bytes properly.
                let downstream_id = u16::from_le_bytes(
                    connection_info_body[message_head..(message_head + mem::size_of::<u16>())]
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
        }
        true
    }

    fn receive_udp_message_and_set_up_clock_sync(
        &mut self,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
    ) -> bool {
        // Read the MsgType::UdpPort message from the federate_info regardless of the status of
        // clock synchronization. This message will tell the RTI whether the federate_info
        // is doing clock synchronization, and if it is, what port to use for UDP.
        println!(
            "RTI waiting for MsgType::UdpPort from federate_info {}.",
            fed_id
        );
        let mut response = vec![0 as u8; 1 + mem::size_of::<u16>()];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut response,
            fed_id,
            "MsgType::UdpPort message",
        );
        if response[0] != MsgType::UdpPort.to_byte() {
            println!("RTI was expecting a MsgType::UdpPort message from federate_info {}. Got {} instead. Rejecting federate_info.", fed_id, response[0]);
            Self::send_reject(stream, ErrType::UnexpectedMessage.to_byte());
            return false;
        } else {
            let cloned_rti = Arc::clone(&_f_rti);
            let clock_sync_global_status;
            {
                let locked_rti = cloned_rti.lock().unwrap();
                clock_sync_global_status = locked_rti.clock_sync_global_status();
            }

            if clock_sync_global_status >= ClockSyncStat::ClockSyncInit {
                // If no initial clock sync, no need perform initial clock sync.
                // FIXME: Change from_le_bytes properly.
                let federate_udp_port_number =
                    u16::from_le_bytes(response[1..3].try_into().unwrap());

                println!(
                    "RTI got MsgType::UdpPort {} from federate_info {}.",
                    federate_udp_port_number, fed_id
                );
                // A port number of UINT16_MAX means initial clock sync should not be performed.
                if federate_udp_port_number != u16::MAX {
                    // TODO: Implement this if body
                    println!(
                        "RTI finished initial clock synchronization with federate_info {}.",
                        fed_id
                    );
                }
                if clock_sync_global_status >= ClockSyncStat::ClockSyncOn {
                    // If no runtime clock sync, no need to set up the UDP port.
                    if federate_udp_port_number > 0 {
                        // Initialize the UDP_addr field of the federate_info struct
                        // TODO: Handle below assignments
                        // fed.UDP_addr.sin_family = AF_INET;
                        // fed.UDP_addr.sin_port = htons(federate_udp_port_number);
                        // fed.UDP_addr.sin_addr = fed->server_ip_addr;
                    }
                } else {
                    // Disable clock sync after initial round.
                    let mut locked_rti = cloned_rti.lock().unwrap();
                    let idx: usize = fed_id.into();
                    let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
                    fed.set_clock_synchronization_enabled(false);
                }
            } else {
                // No clock synchronization at all.
                // Clock synchronization is universally disabled via the clock-sync command-line parameter
                // (-c off was passed to the RTI).
                // Note that the federates are still going to send a MSG_TYPE_UdpPort message but with a payload (port) of -1.
                let mut locked_rti = cloned_rti.lock().unwrap();
                let idx: usize = fed_id.into();
                let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
                fed.set_clock_synchronization_enabled(false);
            }
        }
        true
    }

    fn handle_timestamp(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        received_start_times: Arc<(Mutex<bool>, Condvar)>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut buffer = vec![0 as u8; mem::size_of::<i64>()];
        let bytes_read = NetUtil::read_from_stream(stream, &mut buffer, fed_id);
        if bytes_read < mem::size_of::<i64>() {
            println!("ERROR reading timestamp from federate_info {}.", fed_id);
        }

        // FIXME: Check whether swap_bytes_if_big_endian_int64() is implemented correctly
        let timestamp = i64::from_le_bytes(buffer.try_into().unwrap());
        println!("RTI received timestamp message with time: {} .", timestamp);

        let mut num_feds_proposed_start;
        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
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
                    let locked_rti = _f_rti.lock().unwrap();
                    num_feds_proposed_start = locked_rti.num_feds_proposed_start();
                }
            }
        }

        // Send back to the federate_info the maximum time plus an offset on a Timestamp
        // message.
        let mut start_time_buffer = vec![0 as u8; MSG_TYPE_TIMESTAMP_LENGTH];
        start_time_buffer[0] = MsgType::Timestamp.to_byte();
        // Add an offset to this start time to get everyone starting together.
        let max_start_time;
        {
            let locked_rti = _f_rti.lock().unwrap();
            max_start_time = locked_rti.max_start_time();
        }
        let mut locked_start_time = start_time.lock().unwrap();
        locked_start_time.set_start_time(max_start_time + net_common::DELAY_START);
        // TODO: Consider swap_bytes_if_big_endian_int64()
        NetUtil::encode_int64(locked_start_time.start_time(), &mut start_time_buffer, 1);

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            let stream = my_fed.stream().as_ref().unwrap();
            let bytes_written = NetUtil::write_to_stream(stream, &start_time_buffer, fed_id);
            if bytes_written < MSG_TYPE_TIMESTAMP_LENGTH {
                println!(
                    "Failed to send the starting time to federate_info {}.",
                    fed_id
                );
            }

            // Update state for the federate_info to indicate that the MSG_TYPE_Timestamp
            // message has been sent. That MSG_TYPE_Timestamp message grants time advance to
            // the federate_info to the start time.
            my_fed.enclave().set_state(SchedulingNodeState::Granted);
            let sent_start_time_notifier = Arc::clone(&sent_start_time);
            let (lock, condvar) = &*sent_start_time_notifier;
            let mut notified = lock.lock().unwrap();
            *notified = true;
            condvar.notify_all();
            println!(
                "RTI sent start time {} to federate_info {}.",
                locked_start_time.start_time(),
                my_fed.enclave().id()
            );
        }
    }

    fn handle_federate_resign(
        fed_id: u16,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        // Nothing more to do. Close the socket and exit.

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            my_fed
                .enclave()
                .set_state(SchedulingNodeState::NotConnected);
        }

        // Indicate that there will no further events from this federate_info.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            my_fed.enclave().set_next_event(Tag::forever_tag());
        }

        // According to this: https://stackoverflow.com/questions/4160347/close-vs-shutdown-socket,
        // the close should happen when receiving a 0 length message from the other end.
        // Here, we just signal the other side that no further writes to the socket are
        // forthcoming, which should result in the other end getting a zero-length reception.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            my_fed
                .stream()
                .as_ref()
                .unwrap()
                .shutdown(Shutdown::Both)
                .unwrap();
        }

        println!("FederateInfo {} has resigned.", fed_id);

        // Check downstream federates to see whether they should now be granted a TAG.
        // To handle cycles, need to create a boolean array to keep
        // track of which upstream federates have been visited.
        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        let mut visited = vec![false as bool; number_of_enclaves as usize]; // Initializes to 0.
        SchedulingNode::notify_downstream_advance_grant_if_safe(
            _f_rti.clone(),
            fed_id,
            number_of_enclaves,
            start_time_value,
            &mut visited,
            sent_start_time,
        );
    }

    fn handle_timed_message(
        message_type: u8,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let header_size = 1
            + mem::size_of::<u16>()
            + mem::size_of::<u16>()
            + mem::size_of::<i32>()
            + mem::size_of::<i64>()
            + mem::size_of::<u32>();
        // Read the header, minus the first byte which has already been read.
        let mut header_buffer = vec![0 as u8; (header_size - 1) as usize];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut header_buffer,
            fed_id,
            "the timed message header",
        );
        // Extract the header information. of the sender
        let mut reactor_port_id: u16 = 0;
        let mut federate_id: u16 = 0;
        let mut length: i32 = 0;
        let mut intended_tag = Tag::never_tag();
        // Extract information from the header.
        NetUtil::extract_timed_header(
            &header_buffer[0..],
            &mut reactor_port_id,
            &mut federate_id,
            &mut length,
            &mut intended_tag,
        );

        // FIXME: Handle "as i32" properly.
        let total_bytes_to_read = length + header_size as i32;
        let mut bytes_to_read = length;

        if FED_COM_BUFFER_SIZE < header_size + 1 {
            println!(
                "Buffer size ({}) is not large enough to read the header plus one byte.",
                FED_COM_BUFFER_SIZE
            );
            // FIXME: Change return to exit.
            return;
        }

        // Cut up the payload in chunks.
        // FIXME: Handle unwrap properly.
        let size_diff = (FED_COM_BUFFER_SIZE - header_size).try_into().unwrap();
        if bytes_to_read > size_diff {
            bytes_to_read = size_diff
        }

        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        println!("RTI received message from federate_info {} for federate_info {} port {} with intended tag ({}, {}). Forwarding.",
                fed_id, federate_id, reactor_port_id,
                intended_tag.time() - start_time_value, intended_tag.microstep());

        let mut message_buffer = vec![0 as u8; bytes_to_read.try_into().unwrap()];
        NetUtil::read_from_stream_errexit(stream, &mut message_buffer, fed_id, "timed message");
        // FIXME: Handle "as i32" properly.
        let bytes_read = bytes_to_read + header_size as i32;
        // Following only works for string messages.
        // println!("Message received by RTI: {}.", buffer + header_size);

        let completed;
        {
            // Need to acquire the mutex lock to ensure that the thread handling
            // messages coming from the socket connected to the destination does not
            // issue a TAG before this message has been forwarded.
            let mut locked_rti = _f_rti.lock().unwrap();

            // If the destination federate_info is no longer connected, issue a warning
            // and return.
            let idx: usize = federate_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            let enclave = fed.enclave();
            if enclave.state() == SchedulingNodeState::NotConnected {
                println!(
                    "RTI: Destination federate_info {} is no longer connected. Dropping message.",
                    federate_id
                );
                println!("Fed status: next_event ({}, {}), completed ({}, {}), last_granted ({}, {}), last_provisionally_granted ({}, {}).",
                        enclave.next_event().time() - start_time_value,
                        enclave.next_event().microstep(),
                        enclave.completed().time() - start_time_value,
                        enclave.completed().microstep(),
                        enclave.last_granted().time() - start_time_value,
                        enclave.last_granted().microstep(),
                        enclave.last_provisionally_granted().time() - start_time_value,
                        enclave.last_provisionally_granted().microstep()
                );
                return;
            }

            completed = enclave.completed();
        }

        println!(
            "RTI forwarding message to port {} of federate_info {} of length {}.",
            reactor_port_id, federate_id, length
        );

        // Record this in-transit message in federate_info's in-transit message queue.
        if Tag::lf_tag_compare(&completed, &intended_tag) < 0 {
            // Add a record of this message to the list of in-transit messages to this federate_info.
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = federate_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            MessageRecord::add_in_transit_message_record(
                fed.in_transit_message_tags(),
                intended_tag.clone(),
            );
            println!(
                "RTI: Adding a message with tag ({}, {}) to the list of in-transit messages for federate_info {}.",
                intended_tag.time() - start_time_value,
                intended_tag.microstep(),
                federate_id
            );
        } else {
            println!(
                "RTI: FederateInfo {} has already completed tag ({}, {}), but there is an in-transit message with tag ({}, {}) from federate_info {}. This is going to cause an STP violation under centralized coordination.",
                federate_id,
                completed.time() - start_time_value,
                completed.microstep(),
                intended_tag.time() - start_time_value,
                intended_tag.microstep(),
                fed_id
            );
            // FIXME: Drop the federate_info?
        }

        // Need to make sure that the destination federate_info's thread has already
        // sent the starting MsgType::Timestamp message.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = federate_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            while fed.enclave().state() == SchedulingNodeState::Pending {
                // Need to wait here.
                let (lock, condvar) = &*sent_start_time;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
            }

            // FIXME: Handle unwrap properly.
            let destination_stream = fed.stream().as_ref().unwrap();
            let mut result_buffer = vec![0 as u8; 1];
            result_buffer[0] = message_type;
            result_buffer = vec![result_buffer.clone(), header_buffer, message_buffer].concat();
            NetUtil::write_to_stream_errexit(
                destination_stream,
                &result_buffer,
                federate_id,
                "message",
            );
        }

        // The message length may be longer than the buffer,
        // in which case we have to handle it in chunks.
        let mut total_bytes_read = bytes_read;
        while total_bytes_read < total_bytes_to_read {
            println!("Forwarding message in chunks.");
            bytes_to_read = total_bytes_to_read - total_bytes_read;
            // FIXME: Handle "as i32" properly.
            let fed_com_buffer_size = FED_COM_BUFFER_SIZE as i32;
            if bytes_to_read > fed_com_buffer_size {
                bytes_to_read = fed_com_buffer_size;
            }
            let mut forward_buffer = vec![0 as u8; bytes_to_read as usize];
            NetUtil::read_from_stream_errexit(
                stream,
                &mut forward_buffer,
                fed_id,
                "message chunks",
            );
            total_bytes_read += bytes_to_read;

            // FIXME: a mutex needs to be held for this so that other threads
            // do not write to destination_socket and cause interleaving. However,
            // holding the rti_mutex might be very expensive. Instead, each outgoing
            // socket should probably have its own mutex.
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                let idx: usize = federate_id.into();
                let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
                // FIXME: Handle unwrap properly.
                let destination_stream = fed.stream().as_ref().unwrap();
                NetUtil::write_to_stream_errexit(
                    destination_stream,
                    &forward_buffer,
                    federate_id,
                    "message chunks",
                );
            }
        }

        Self::update_federate_next_event_tag_locked(
            _f_rti,
            federate_id,
            intended_tag,
            start_time_value,
            sent_start_time,
        );
    }

    fn update_federate_next_event_tag_locked(
        _f_rti: Arc<Mutex<RTIRemote>>,
        fed_id: u16,
        mut next_event_tag: Tag,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let min_in_transit_tag;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            min_in_transit_tag = MessageRecord::get_minimum_in_transit_message_tag(
                fed.in_transit_message_tags(),
                start_time,
            );
        }
        if Tag::lf_tag_compare(&min_in_transit_tag, &next_event_tag) < 0 {
            next_event_tag = min_in_transit_tag.clone();
        }
        SchedulingNode::update_scheduling_node_next_event_tag_locked(
            _f_rti,
            fed_id,
            next_event_tag,
            start_time,
            sent_start_time,
        );
    }

    fn handle_next_event_tag(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut header_buffer = vec![0 as u8; mem::size_of::<i64>() + mem::size_of::<u32>()];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut header_buffer,
            fed_id,
            "the content of the next event tag",
        );

        // Acquire a mutex lock to ensure that this state does not change while a
        // message is in transport or being used to determine a TAG.
        let enclave_id;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            enclave_id = fed.enclave().id();
        }
        let intended_tag = NetUtil::extract_tag(
            header_buffer[0..(mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        println!(
            "RTI received from federate_info {} the Next Event Tag (NET) ({},{})",
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

    fn handle_logical_tag_complete(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut header_buffer = vec![0 as u8; mem::size_of::<i64>() + mem::size_of::<u32>()];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut header_buffer,
            fed_id,
            "the content of the logical tag complete",
        );
        let completed = NetUtil::extract_tag(
            header_buffer[0..(mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        SchedulingNode::logical_tag_complete(
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
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            let in_transit_message_tags = fed.in_transit_message_tags();
            MessageRecord::clean_in_transit_message_record_up_to_tag(
                in_transit_message_tags,
                completed,
                start_time_value,
            );
        }
    }

    fn handle_stop_request_message(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        stop_granted: Arc<Mutex<StopGranted>>,
    ) {
        println!("RTI handling stop_request from federate_info {}.", fed_id);

        let mut header_buffer = vec![0 as u8; MSG_TYPE_STOP_REQUEST_LENGTH - 1];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut header_buffer,
            fed_id,
            "the MsgType::StopRequest payload",
        );

        // Acquire a mutex lock to ensure that this state does change while a
        // message is in transport or being used to determine a TAG.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];

            // Check whether we have already received a stop_tag
            // from this federate_info
            if fed.requested_stop() {
                // Ignore this request
                return;
            }
        }

        // Extract the proposed stop tag for the federate_info
        let proposed_stop_tag = NetUtil::extract_tag(
            header_buffer[0..(mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        // Update the maximum stop tag received from federates
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            if Tag::lf_tag_compare(&proposed_stop_tag, &locked_rti.base().max_stop_tag()) > 0 {
                locked_rti
                    .base()
                    .set_max_stop_tag(proposed_stop_tag.clone());
            }
        }

        println!(
            "RTI received from federate_info {} a MsgType::StopRequest message with tag ({},{}).",
            fed_id,
            proposed_stop_tag.time() - start_time_value,
            proposed_stop_tag.microstep()
        );

        // If this federate_info has not already asked
        // for a stop, add it to the tally.
        Self::mark_federate_requesting_stop(
            fed_id,
            _f_rti.clone(),
            stop_granted.clone(),
            start_time_value,
        );

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            if locked_rti.base().num_scheduling_nodes_handling_stop()
                == locked_rti.base().number_of_scheduling_nodes()
            {
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
                locked_rti.base().max_stop_tag().time(),
                locked_rti.base().max_stop_tag().microstep(),
            );
        }

        // Iterate over federates and send each the MSG_TYPE_StopRequest message
        // if we do not have a stop_time already for them. Do not do this more than once.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            if locked_rti.stop_in_progress() {
                return;
            }
            locked_rti.set_stop_in_progress(true);
        }
        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        for i in 0..number_of_enclaves {
            let mut locked_rti = _f_rti.lock().unwrap();
            let f: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[i as usize];
            if f.e().id() != fed_id && f.requested_stop() == false {
                if f.e().state() == SchedulingNodeState::NotConnected {
                    Self::mark_federate_requesting_stop(
                        f.e().id(),
                        _f_rti.clone(),
                        stop_granted.clone(),
                        start_time_value,
                    );
                    continue;
                }
                // FIXME: Handle unwrap properly.
                let stream = f.stream().as_ref().unwrap();
                NetUtil::write_to_stream_errexit(
                    stream,
                    &stop_request_buffer,
                    f.e().id(),
                    "MsgType::StopRequest message",
                );
            }
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            println!(
                "RTI forwarded to federates MsgType::StopRequest with tag ({}, {}).",
                locked_rti.base().max_stop_tag().time() - start_time_value,
                locked_rti.base().max_stop_tag().microstep()
            );
        }
    }

    fn mark_federate_requesting_stop(
        fed_id: u16,
        _f_rti: Arc<Mutex<RTIRemote>>,
        stop_granted: Arc<Mutex<StopGranted>>,
        start_time_value: Instant,
    ) {
        let mut num_enclaves_handling_stop;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            num_enclaves_handling_stop = locked_rti.base().num_scheduling_nodes_handling_stop();
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            if !fed.requested_stop() {
                // Assume that the federate_info
                // has requested stop
                locked_rti
                    .base()
                    .set_num_scheduling_nodes_handling_stop(num_enclaves_handling_stop + 1);
            }
        }
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            if !fed.requested_stop() {
                // Assume that the federate_info
                // has requested stop
                fed.set_requested_stop(true);
            }
        }
        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            num_enclaves_handling_stop = locked_rti.base().num_scheduling_nodes_handling_stop();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
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
     * it will broadcast a MSG_TYPE_StopGranted carrying the _RTI.max_stop_tag.
     * This function also checks the most recently received NET from
     * each federate_info and resets that be no greater than the _RTI.max_stop_tag.
     *
     * This function assumes the caller holds the _RTI.rti_mutex lock.
     */
    fn _lf_rti_broadcast_stop_time_to_federates_locked(
        _f_rti: Arc<Mutex<RTIRemote>>,
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
                locked_rti.base().max_stop_tag().time(),
                locked_rti.base().max_stop_tag().microstep(),
            );
        }

        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        // Iterate over federates and send each the message.
        for i in 0..number_of_enclaves {
            let next_event;
            let max_stop_tag;
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                max_stop_tag = locked_rti.base().max_stop_tag();
                // FIXME: Handle usize properly.
                let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[i as usize];
                next_event = fed.e().next_event();
                if fed.e().state() == SchedulingNodeState::NotConnected {
                    continue;
                }
            }
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[i as usize];
                if Tag::lf_tag_compare(&next_event, &max_stop_tag) >= 0 {
                    // Need the next_event to be no greater than the stop tag.
                    fed.enclave().set_next_event(max_stop_tag);
                }
            }
            {
                let mut locked_rti = _f_rti.lock().unwrap();
                let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[i as usize];
                // FIXME: Handle unwrap properly.
                let stream = fed.stream().as_ref().unwrap();
                NetUtil::write_to_stream_errexit(
                    stream,
                    &outgoing_buffer,
                    fed.e().id(),
                    "MsgType::StopGranted message",
                );
            }
        }

        {
            let mut locked_rti = _f_rti.lock().unwrap();
            println!(
                "RTI sent to federates MsgType::StopGranted with tag ({}, {}).",
                locked_rti.base().max_stop_tag().time() - start_time_value,
                locked_rti.base().max_stop_tag().microstep()
            );
        }
        {
            let mut _stop_granted = stop_granted.lock().unwrap();
            _stop_granted.set_lf_rti_stop_granted_already_sent_to_federates(true);
        }
    }

    // FIXME: Replace this function to a macro if needed.
    fn encode_stop_granted(outgoing_buffer: &mut Vec<u8>, time: Instant, microstep: Microstep) {
        outgoing_buffer[0] = MsgType::StopGranted.to_byte();
        NetUtil::encode_int64(time, outgoing_buffer, 1);
        NetUtil::encode_int32(
            microstep as i32,
            outgoing_buffer,
            1 + mem::size_of::<Instant>(),
        );
    }

    // FIXME: Replace this function to a macro if needed.
    fn encode_stop_request(stop_request_buffer: &mut Vec<u8>, time: Instant, microstep: Microstep) {
        stop_request_buffer[0] = MsgType::StopRequest.to_byte();
        NetUtil::encode_int64(time, stop_request_buffer, 1);
        NetUtil::encode_int32(
            microstep as i32,
            stop_request_buffer,
            1 + mem::size_of::<Instant>(),
        );
    }

    fn handle_stop_request_reply(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        stop_granted: Arc<Mutex<StopGranted>>,
    ) {
        let mut header_buffer = vec![0 as u8; MSG_TYPE_STOP_REQUEST_REPLY_LENGTH - 1];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut header_buffer,
            fed_id,
            "the reply to MsgType::StopRequest message",
        );

        let federate_stop_tag = NetUtil::extract_tag(
            header_buffer[0..(MSG_TYPE_STOP_REQUEST_REPLY_LENGTH - 1)]
                .try_into()
                .unwrap(),
        );

        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        println!(
            "RTI received from federate_info {} STOP reply tag ({}, {}).",
            fed_id,
            federate_stop_tag.time() - start_time_value,
            federate_stop_tag.microstep()
        );

        // Acquire the mutex lock so that we can change the state of the RTI
        // If the federate_info has not requested stop before, count the reply
        let max_stop_tag;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            max_stop_tag = locked_rti.base().max_stop_tag();
        }
        if Tag::lf_tag_compare(&federate_stop_tag, &max_stop_tag) > 0 {
            let mut locked_rti = _f_rti.lock().unwrap();
            locked_rti.base().set_max_stop_tag(federate_stop_tag);
        }
        Self::mark_federate_requesting_stop(
            fed_id,
            _f_rti.clone(),
            stop_granted.clone(),
            start_time_value,
        );
    }

    fn handle_port_absent_message(
        buffer: &Vec<u8>,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<Mutex<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let message_size =
            mem::size_of::<u16>() * 2 + mem::size_of::<i64>() + mem::size_of::<u32>();

        let mut header_buffer = vec![0 as u8; message_size];
        NetUtil::read_from_stream_errexit(
            stream,
            &mut header_buffer,
            fed_id,
            "port absent message",
        );

        let u16_size = mem::size_of::<u16>();
        // FIXME: Change from_le_bytes properly.
        let reactor_port_id = u16::from_le_bytes(header_buffer[0..(u16_size)].try_into().unwrap());
        let federate_id = u16::from_le_bytes(
            header_buffer[(u16_size)..(u16_size + u16_size)]
                .try_into()
                .unwrap(),
        );

        // TODO: Will be used when tracing_enabled
        // let start_idx = u16_size * 2;
        // let tag = NetUtil::extract_tag(
        //     header_buffer[start_idx..(start_idx + mem::size_of::<i64>() + mem::size_of::<u32>())]
        //         .try_into()
        //         .unwrap(),
        // );

        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        // Need to acquire the mutex lock to ensure that the thread handling
        // messages coming from the socket connected to the destination does not
        // issue a TAG before this message has been forwarded.
        {
            let mut locked_rti = _f_rti.lock().unwrap();

            // If the destination federate_info is no longer connected, issue a warning
            // and return.
            let idx: usize = federate_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            let enclave = fed.enclave();
            if enclave.state() == SchedulingNodeState::NotConnected {
                println!(
                    "RTI: Destination federate_info {} is no longer connected. Dropping message.",
                    federate_id
                );
                println!("Fed status: next_event ({}, {}), completed ({}, {}), last_granted ({}, {}), last_provisionally_granted ({}, {}).",
                        enclave.next_event().time() - start_time_value,
                        enclave.next_event().microstep(),
                        enclave.completed().time() - start_time_value,
                        enclave.completed().microstep(),
                        enclave.last_granted().time(), // TODO: - start_time_value,
                        enclave.last_granted().microstep(),
                        enclave.last_provisionally_granted().time() - start_time_value,
                        enclave.last_provisionally_granted().microstep()
                );
                return;
            }
        }
        println!(
            "RTI forwarding port absent message for port {} to federate_info {}.",
            reactor_port_id, federate_id
        );

        // Need to make sure that the destination federate_info's thread has already
        // sent the starting MsgType::Timestamp message.
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let idx: usize = federate_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base().scheduling_nodes()[idx];
            while fed.enclave().state() == SchedulingNodeState::Pending {
                // Need to wait here.
                let (lock, condvar) = &*sent_start_time;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
            }

            // Forward the message.
            let destination_stream = fed.stream().as_ref().unwrap();
            let mut result_buffer = vec![0 as u8];
            result_buffer[0] = buffer[0];
            result_buffer = vec![result_buffer.clone(), header_buffer].concat();
            NetUtil::write_to_stream_errexit(
                destination_stream,
                &result_buffer,
                federate_id,
                "message",
            );
        }
    }
}
