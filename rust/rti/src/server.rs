/**
 * @file
 * @author Hokeun Kim (hokeun@asu.edu)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @copyright (c) 2023-2024, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::io::Write;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::in_transit_message_queue::InTransitMessageQueue;
use crate::net_common;
use crate::net_common::*;
use crate::net_util::*;
use crate::tag;
use crate::tag::*;
use crate::trace::{TraceDirection, TraceEvent};
use crate::ClockSyncStat;
use crate::FederateInfo;
use crate::RTIRemote;
use crate::SchedulingNode;
use crate::SchedulingNodeState;
use crate::Trace;

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
    socket_type: SocketType,
}

impl Server {
    pub fn create_rti_server(
        rti_remote: &mut RTIRemote,
        port: u16,
        socket_type: SocketType,
    ) -> Server {
        let mut type_str = String::from("TCP");
        if socket_type == SocketType::UDP {
            type_str = String::from("UDP");

            let mut address = String::from("0.0.0.0:");
            address.push_str(port.to_string().as_str());
            // TODO: Handle unwrap() properly.
            let socket = UdpSocket::bind(address).unwrap();
            rti_remote.set_socket_descriptor_udp(Some(socket));
        }
        println!(
            "RTI using {} port {} for federation {}.",
            type_str,
            port,
            rti_remote.federation_id()
        );

        if socket_type == SocketType::TCP {
            rti_remote.set_final_port_tcp(port);
        } else if socket_type == SocketType::UDP {
            rti_remote.set_final_port_udp(port);
            // No need to listen on the UDP socket
        }

        Server {
            port: port.to_string(),
            socket_type,
        }
    }

    pub fn socket_type(&self) -> SocketType {
        self.socket_type.clone()
    }

    pub fn wait_for_federates(&mut self, _f_rti: RTIRemote) {
        println!("Server listening on port {}", self.port);
        let mut address = String::from("0.0.0.0:");
        address.push_str(self.port.as_str());
        // TODO: Handle unwrap() properly.
        let socket = TcpListener::bind(address).unwrap();
        let start_time = Arc::new(Mutex::new(StartTime::new()));
        let received_start_times = Arc::new((Mutex::new(false), Condvar::new()));
        let sent_start_time = Arc::new((Mutex::new(false), Condvar::new()));
        let stop_granted = Arc::new(Mutex::new(StopGranted::new()));
        // Wait for connections from federates and create a thread for each.
        let handles = self.lf_connect_to_federates(
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

    fn lf_connect_to_federates(
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
        let arc_rti = Arc::new(RwLock::new(_f_rti));
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
                                    let mut locked_rti = cloned_rti.write().unwrap();
                                    let fed: &mut FederateInfo =
                                        &mut locked_rti.base_mut().scheduling_nodes_mut()
                                            [fed_id as usize];
                                    fed.set_stream(stream.try_clone().unwrap());
                                }

                                // Buffer for incoming messages.
                                // This does not constrain the message size because messages
                                // are forwarded piece by piece.
                                let mut buffer = vec![0 as u8; 1];

                                // Listen for messages from the federate_info.
                                loop {
                                    {
                                        let locked_rti = cloned_rti.read().unwrap();
                                        let enclaves = locked_rti.base().scheduling_nodes();
                                        let fed = &enclaves[fed_id as usize];
                                        let enclave = fed.enclave();
                                        if enclave.state() == SchedulingNodeState::NotConnected {
                                            break;
                                        }
                                    }
                                    // Read no more than one byte to get the message type.
                                    // FIXME: Handle unwrap properly.
                                    let bytes_read = NetUtil::read_from_socket(
                                        &mut stream,
                                        &mut buffer,
                                        fed_id.try_into().unwrap(),
                                    );
                                    if bytes_read < 1 {
                                        // Socket is closed
                                        println!("RTI: Socket to federate_info {} is closed. Exiting the thread.",
                                            fed_id);
                                        let mut locked_rti = cloned_rti.write().unwrap();
                                        let enclaves = locked_rti.base_mut().scheduling_nodes_mut();
                                        let fed: &mut FederateInfo = &mut enclaves[fed_id as usize];
                                        fed.enclave_mut()
                                            .set_state(SchedulingNodeState::NotConnected);
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
                                        MsgType::LatestTagComplete => {
                                            Self::handle_latest_tag_complete(
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
                                        MsgType::AddressQuery => Self::handle_address_query(
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                        ),
                                        MsgType::AddressAdvertisement => Self::handle_address_ad(
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                        ),
                                        MsgType::PortAbsent => Self::handle_port_absent_message(
                                            &buffer,
                                            fed_id.try_into().unwrap(),
                                            &mut stream,
                                            cloned_rti.clone(),
                                            cloned_start_time.clone(),
                                            cloned_sent_start_time.clone(),
                                        ),
                                        _ => {
                                            let locked_rti = cloned_rti.read().unwrap();
                                            let fed = &locked_rti.base().scheduling_nodes()
                                                [fed_id as usize];
                                            println!("RTI received from federate_info {} an unrecognized TCP message type: {}.", fed.enclave().id(), buffer[0]);
                                            Trace::log_trace(
                                                cloned_rti.clone(),
                                                TraceEvent::ReceiveUnidentified,
                                                fed_id.try_into().unwrap(),
                                                &Tag::forever_tag(),
                                                i64::MAX,
                                                TraceDirection::From,
                                            );
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
        let clock_sync_global_status;
        let number_of_scheduling_nodes;
        let final_port_udp;
        {
            let locked_rti = cloned_rti.read().unwrap();
            clock_sync_global_status = locked_rti.clock_sync_global_status();
            number_of_scheduling_nodes = locked_rti.base().number_of_scheduling_nodes();
            final_port_udp = locked_rti.final_port_udp();
        }
        if clock_sync_global_status >= ClockSyncStat::ClockSyncOn {
            // Create the thread that performs periodic PTP clock synchronization sessions
            // over the UDP channel, but only if the UDP channel is open and at least one
            // federate_info is performing runtime clock synchronization.
            let mut clock_sync_enabled = false;
            for i in 0..number_of_scheduling_nodes {
                {
                    let locked_rti = cloned_rti.read().unwrap();
                    if locked_rti.base().scheduling_nodes()[i as usize]
                        .clock_synchronization_enabled()
                    {
                        clock_sync_enabled = true;
                        break;
                    }
                }
            }
            // let cloned_start_time = Arc::clone(&start_time);
            // let cloned_received_start_times = Arc::clone(&received_start_times);

            if final_port_udp != u16::MAX && clock_sync_enabled {
                let handle = thread::spawn(move || {
                    // Wait until all federates have been notified of the start time.
                    // FIXME: Use lf_ version of this when merged with master.
                    {
                        let locked_rti = cloned_rti.read().unwrap();
                        while locked_rti.num_feds_proposed_start()
                            < locked_rti.base().number_of_scheduling_nodes()
                        {
                            // Need to wait here.
                            let received_start_times_notifier = Arc::clone(&received_start_times);
                            let (lock, condvar) = &*received_start_times_notifier;
                            let mut notified = lock.lock().unwrap();
                            while !*notified {
                                notified = condvar.wait(notified).unwrap();
                            }
                        }
                    }

                    // Wait until the start time before starting clock synchronization.
                    // The above wait ensures that start_time has been set.
                    let start_time_value;
                    {
                        let locked_start_time = start_time.lock().unwrap();
                        start_time_value = locked_start_time.start_time();
                    }
                    let ns_to_wait = start_time_value - Tag::lf_time_physical();

                    if ns_to_wait > 0 {
                        // TODO: Handle unwrap() properly.
                        let ns = Duration::from_nanos(ns_to_wait.try_into().unwrap());
                        thread::sleep(ns);
                    }

                    // Initiate a clock synchronization every rti->clock_sync_period_ns
                    // let sleep_time = {(time_t)rti_remote->clock_sync_period_ns / BILLION,
                    //                               rti_remote->clock_sync_period_ns % BILLION};
                    // let remaining_time;

                    let mut any_federates_connected = true;
                    while any_federates_connected {
                        // Sleep
                        let clock_sync_period_ns;
                        let number_of_scheduling_nodes;
                        {
                            let locked_rti = cloned_rti.read().unwrap();
                            clock_sync_period_ns = locked_rti.clock_sync_period_ns();
                            number_of_scheduling_nodes =
                                locked_rti.base().number_of_scheduling_nodes();
                        }
                        let ns = Duration::from_nanos(clock_sync_period_ns); // Can be interrupted
                        thread::sleep(ns);
                        any_federates_connected = false;
                        for fed_id in 0..number_of_scheduling_nodes {
                            let state;
                            let clock_synchronization_enabled;
                            {
                                let locked_rti = cloned_rti.read().unwrap();
                                let idx: usize = fed_id as usize;
                                let fed = &locked_rti.base().scheduling_nodes()[idx];
                                state = fed.enclave().state();
                                clock_synchronization_enabled = fed.clock_synchronization_enabled();
                            }
                            if state == SchedulingNodeState::NotConnected {
                                // FIXME: We need better error handling here, but clock sync failure
                                // should not stop execution.
                                println!(
                                    "[ERROR] Clock sync failed with federate {}. Not connected.",
                                    fed_id
                                );
                                continue;
                            } else if !clock_synchronization_enabled {
                                continue;
                            }
                            // Send the RTI's current physical time to the federate
                            // Send on UDP.
                            println!(
                                "[DEBUG] RTI sending T1 message to initiate clock sync round."
                            );
                            // TODO: Handle unwrap() properly.
                            Self::send_physical_clock_with_udp(
                                fed_id.try_into().unwrap(),
                                cloned_rti.clone(),
                                MsgType::ClockSyncT1.to_byte(),
                            );

                            // Listen for reply message, which should be T3.
                            let message_size = 1 + std::mem::size_of::<i32>();
                            let mut buffer = vec![0 as u8; message_size];
                            // Maximum number of messages that we discard before giving up on this cycle.
                            // If the T3 message from this federate does not arrive and we keep receiving
                            // other messages, then give up on this federate and move to the next federate.
                            let mut remaining_attempts = 5;
                            while remaining_attempts > 0 {
                                remaining_attempts -= 1;
                                let mut read_failed = true;
                                {
                                    let mut locked_rti = cloned_rti.write().unwrap();
                                    // TODO: Handle unwrap() properly.
                                    let udp_socket =
                                        locked_rti.socket_descriptor_udp().as_mut().unwrap();
                                    match udp_socket.recv(&mut buffer) {
                                        Ok(read_bytes) => {
                                            if read_bytes > 0 {
                                                read_failed = false;
                                            }
                                        }
                                        Err(..) => {
                                            println!("[ERROR] Failed to read from an UDP socket.");
                                        }
                                    }
                                }
                                // If any errors occur, either discard the message or the clock sync round.
                                if !read_failed {
                                    if buffer[0] == MsgType::ClockSyncT3.to_byte() {
                                        // TODO: Change from_le_bytes properly.
                                        let fed_id_2 = i32::from_le_bytes(
                                            buffer[1..1 + std::mem::size_of::<i32>()]
                                                .try_into()
                                                .unwrap(),
                                        );
                                        // Check that this message came from the correct federate.
                                        if fed_id_2 != fed_id {
                                            // Message is from the wrong federate. Discard the message.
                                            println!("[WARNING] Clock sync: Received T3 message from federate {}, but expected one from {}. Discarding message.",
                                                        fed_id_2, fed_id);
                                            continue;
                                        }
                                        println!("[DEBUG] Clock sync: RTI received T3 message from federate {}.", fed_id_2);
                                        // TODO: Handle unwrap() properly.
                                        Self::handle_physical_clock_sync_message_with_udp(
                                            fed_id_2.try_into().unwrap(),
                                            cloned_rti.clone(),
                                        );
                                        break;
                                    } else {
                                        // The message is not a T3 message. Discard the message and
                                        // continue waiting for the T3 message. This is possibly a message
                                        // from a previous cycle that was discarded.
                                        println!("[WARNING] Clock sync: Unexpected UDP message {}. Expected MsgType::ClockSyncT3 from federate {}. Discarding message.",
                                                    buffer[0], fed_id);
                                        continue;
                                    }
                                } else {
                                    println!("[WARNING] Clock sync: Read from UDP socket failed: Skipping clock sync round for federate {}.",
                                                fed_id);
                                    remaining_attempts -= 1;
                                }
                            }
                            if remaining_attempts > 0 {
                                any_federates_connected = true;
                            }
                        }
                    }
                });
                handle_list.push(handle);
            }
        }

        handle_list
    }

    fn receive_and_check_fed_id_message(
        &mut self,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
    ) -> i32 {
        // Buffer for message ID, federate_info ID, and federation ID length.
        let length = 1 + mem::size_of::<u16>() + 1;
        let mut first_buffer = vec![0 as u8; length];

        // Read bytes from the socket. We need 4 bytes.
        // FIXME: This should not exit with error but rather should just reject the connection.
        NetUtil::read_from_socket_fail_on_error(stream, &mut first_buffer, 0, "");

        // Initialize to an invalid value.
        let mut fed_id = u16::MAX;

        // First byte received is the message type.
        if first_buffer[0] != MsgType::FedIds.to_byte() {
            Trace::log_trace(
                _f_rti.clone(),
                TraceEvent::SendReject,
                fed_id,
                &Tag::forever_tag(),
                i64::MIN,
                TraceDirection::To,
            );
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
            // FIXME: Update from_le_bytes if endian types should be considered.
            let u16_size = mem::size_of::<u16>();
            fed_id = u16::from_le_bytes(first_buffer[1..(1 + u16_size)].try_into().unwrap());
            println!("RTI received federate_info ID: {}.", fed_id);

            // Read the federation ID.  First read the length, which is one byte.
            // FIXME: Update from_le_bytes if endian types should be considered.
            let federation_id_length = u8::from_le_bytes(
                first_buffer[(1 + u16_size)..(1 + u16_size + 1)]
                    .try_into()
                    .unwrap(),
            );
            let mut federation_id_buffer = vec![0 as u8; federation_id_length.into()];
            // Next read the actual federation ID.
            // FIXME: This should not exit on error, but rather just reject the connection.
            NetUtil::read_from_socket_fail_on_error(
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
                let locked_rti = cloned_rti.read().unwrap();
                number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
                federation_id = locked_rti.federation_id();
            }
            Trace::log_trace(
                cloned_rti.clone(),
                TraceEvent::ReceiveFedId,
                fed_id,
                &Tag::forever_tag(),
                i64::MIN,
                TraceDirection::From,
            );
            // Compare the received federation ID to mine.
            if federation_id_received != federation_id {
                // Federation IDs do not match. Send back a MSG_TYPE_Reject message.
                println!(
                    "WARNING: FederateInfo from another federation {} attempted to connect to RTI in federation {}.",
                    federation_id_received, federation_id
                );
                Trace::log_trace(
                    cloned_rti.clone(),
                    TraceEvent::SendReject,
                    fed_id,
                    &Tag::forever_tag(),
                    0,
                    TraceDirection::To,
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
                    Trace::log_trace(
                        cloned_rti.clone(),
                        TraceEvent::SendReject,
                        fed_id,
                        &Tag::forever_tag(),
                        i64::MIN,
                        TraceDirection::To,
                    );
                    Self::send_reject(stream, ErrType::FederateIdOutOfRange.to_byte());
                    return -1;
                } else {
                    let state;
                    {
                        let locked_rti = cloned_rti.read().unwrap();
                        let idx: usize = fed_id.into();
                        let federate_info = &locked_rti.base().scheduling_nodes()[idx];
                        state = federate_info.enclave().state();
                    }
                    if state != SchedulingNodeState::NotConnected {
                        println!("RTI received duplicate federate_info ID: {}.", fed_id);
                        Trace::log_trace(
                            cloned_rti.clone(),
                            TraceEvent::SendReject,
                            fed_id,
                            &Tag::forever_tag(),
                            i64::MIN,
                            TraceDirection::To,
                        );
                        Self::send_reject(stream, ErrType::FederateIdInUse.to_byte());
                        return -1;
                    }
                }
            }
            println!(
                "Federation ID matches! \"{}(received)\" <-> \"{}(_f_rti)\"",
                federation_id_received, federation_id
            );
            {
                let mut locked_rti = cloned_rti.write().unwrap();
                let idx: usize = fed_id.into();
                let federate_info: &mut FederateInfo =
                    &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
                // The MsgType::FedIds message has the right federation ID.
                // Assign the address information for federate.
                // The IP address is stored here as an in_addr struct (in .server_ip_addr) that can be useful
                // to create sockets and can be efficiently sent over the network.
                // First, convert the sockaddr structure into a sockaddr_in that contains an internet address.
                // Then extract the internet address (which is in IPv4 format) and assign it as the federate's socket server
                // TODO: Handle unwrap() in the below line properly.
                federate_info.set_server_ip_addr(stream.peer_addr().unwrap());
            }

            // Set the federate_info's state as pending
            // because it is waiting for the start time to be
            // sent by the RTI before beginning its execution.
            {
                let mut locked_rti = cloned_rti.write().unwrap();
                let idx: usize = fed_id.into();
                let federate_info: &mut FederateInfo =
                    &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
                let enclave: &mut SchedulingNode = federate_info.enclave_mut();
                enclave.set_state(SchedulingNodeState::Pending);
            }
            println!(
                "RTI responding with MsgType::Ack to federate_info {}.",
                fed_id
            );
            // Send an MsgType::Ack message.
            Trace::log_trace(
                cloned_rti.clone(),
                TraceEvent::SendAck,
                fed_id,
                &Tag::forever_tag(),
                i64::MIN,
                TraceDirection::To,
            );
            let ack_message: Vec<u8> = vec![MsgType::Ack.to_byte()];
            NetUtil::write_to_socket_fail_on_error(
                stream,
                &ack_message,
                fed_id,
                "MsgType::Ack message",
            );
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
        _f_rti: Arc<RwLock<RTIRemote>>,
    ) -> bool {
        println!(
            "RTI waiting for MsgType::NeighborStructure from federate_info {}.",
            fed_id
        );
        let cloned_rti = Arc::clone(&_f_rti);
        // TODO: Handle unwrap() properly.
        let mut connection_info_header =
            vec![0 as u8; MSG_TYPE_NEIGHBOR_STRUCTURE_HEADER_SIZE.try_into().unwrap()];
        NetUtil::read_from_socket_fail_on_error(
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
            let mut locked_rti = cloned_rti.write().unwrap();
            let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            let enclave: &mut SchedulingNode = fed.enclave_mut();
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
            NetUtil::read_from_socket_fail_on_error(
                stream,
                &mut connection_info_body,
                fed_id,
                "MsgType::NeighborStructure message body",
            );

            // Keep track of where we are in the buffer
            let mut message_head: usize = 0;
            // First, read the info about upstream federates
            for i in 0..num_upstream {
                // FIXME: Update from_le_bytes if endian types should be considered.
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
                // FIXME: Update from_le_bytes if endian types should be considered.
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
                // FIXME: Update from_le_bytes if endian types should be considered.
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
        _f_rti: Arc<RwLock<RTIRemote>>,
    ) -> bool {
        // Read the MsgType::UdpPort message from the federate_info regardless of the status of
        // clock synchronization. This message will tell the RTI whether the federate_info
        // is doing clock synchronization, and if it is, what port to use for UDP.
        println!(
            "RTI waiting for MsgType::UdpPort from federate_info {}.",
            fed_id
        );
        let mut response = vec![0 as u8; 1 + mem::size_of::<u16>()];
        NetUtil::read_from_socket_fail_on_error(
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
            let clock_sync_exchanges_per_interval;
            {
                let locked_rti = cloned_rti.read().unwrap();
                clock_sync_global_status = locked_rti.clock_sync_global_status();
                clock_sync_exchanges_per_interval = locked_rti.clock_sync_exchanges_per_interval();
            }

            if clock_sync_global_status >= ClockSyncStat::ClockSyncInit {
                // If no initial clock sync, no need perform initial clock sync.
                // FIXME: Update from_le_bytes if endian types should be considered.
                let federate_udp_port_number =
                    u16::from_le_bytes(response[1..3].try_into().unwrap());

                println!(
                    "RTI got MsgType::UdpPort {} from federate_info {}.",
                    federate_udp_port_number, fed_id
                );
                // A port number of UINT16_MAX means initial clock sync should not be performed.
                if federate_udp_port_number != u16::MAX {
                    // Perform the initialization clock synchronization with the federate.
                    // Send the required number of messages for clock synchronization
                    for _i in 0..clock_sync_exchanges_per_interval {
                        // Send the RTI's current physical time T1 to the federate.
                        Self::send_physical_clock_with_tcp(
                            fed_id,
                            _f_rti.clone(),
                            MsgType::ClockSyncT1.to_byte(),
                            stream,
                        );

                        // Listen for reply message, which should be T3.
                        let message_size = 1 + std::mem::size_of::<i32>();
                        let mut buffer = vec![0 as u8; message_size];
                        NetUtil::read_from_socket_fail_on_error(
                            stream,
                            &mut buffer,
                            fed_id,
                            "T3 messages",
                        );
                        if buffer[0] == MsgType::ClockSyncT3.to_byte() {
                            let fed_id = i32::from_le_bytes(
                                buffer[1..1 + std::mem::size_of::<i32>()]
                                    .try_into()
                                    .unwrap(),
                            );
                            println!(
                                "[DEBUG] RTI received T3 clock sync message from federate {}.",
                                fed_id
                            );
                            Self::handle_physical_clock_sync_message_with_tcp(
                                fed_id.try_into().unwrap(),
                                cloned_rti.clone(),
                                stream,
                            );
                        } else {
                            println!(
                                "[ERROR] Unexpected message {} from federate {}.",
                                buffer[0], fed_id
                            );
                            Self::send_reject(stream, ErrType::UnexpectedMessage.to_byte());
                            return false;
                        }
                    }
                    println!(
                        "[DEBUG] RTI finished initial clock synchronization with federate {}.",
                        fed_id
                    );
                }
                if clock_sync_global_status >= ClockSyncStat::ClockSyncOn {
                    // If no runtime clock sync, no need to set up the UDP port.
                    if federate_udp_port_number > 0 {
                        // Initialize the UDP_addr field of the federate_info struct
                        let mut locked_rti = cloned_rti.write().unwrap();
                        let idx: usize = fed_id.into();
                        let fed: &mut FederateInfo =
                            &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
                        fed.set_udp_addr(SocketAddr::new(
                            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                            federate_udp_port_number,
                        ));
                    }
                } else {
                    // Disable clock sync after initial round.
                    let mut locked_rti = cloned_rti.write().unwrap();
                    let idx: usize = fed_id.into();
                    let fed: &mut FederateInfo =
                        &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
                    fed.set_clock_synchronization_enabled(false);
                }
            } else {
                // No clock synchronization at all.
                // Clock synchronization is universally disabled via the clock-sync command-line parameter
                // (-c off was passed to the RTI).
                // Note that the federates are still going to send a MSG_TYPE_UdpPort message but with a payload (port) of -1.
                let mut locked_rti = cloned_rti.write().unwrap();
                let idx: usize = fed_id.into();
                let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
                fed.set_clock_synchronization_enabled(false);
            }
        }
        true
    }

    /**
     * A function to handle timestamp messages.
     * This function assumes the caller does not hold the mutex.
     */
    fn handle_timestamp(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        received_start_times: Arc<(Mutex<bool>, Condvar)>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut buffer = vec![0 as u8; mem::size_of::<i64>()];
        // Read bytes from the socket. We need 8 bytes.
        let bytes_read = NetUtil::read_from_socket(stream, &mut buffer, fed_id);
        if bytes_read < mem::size_of::<i64>() {
            println!("ERROR reading timestamp from federate_info {}.", fed_id);
        }

        // FIXME: Check whether swap_bytes_if_big_endian_int64() is implemented correctly
        let timestamp = i64::from_le_bytes(buffer.try_into().unwrap());
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveTimestamp,
            fed_id,
            &Tag::new(timestamp, 0),
            timestamp,
            TraceDirection::From,
        );
        println!("RTI received timestamp message with time: {} .", timestamp);

        let mut num_feds_proposed_start;
        let number_of_enclaves;
        {
            let mut locked_rti = _f_rti.write().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
            let max_start_time = locked_rti.max_start_time();
            num_feds_proposed_start = locked_rti.num_feds_proposed_start();
            num_feds_proposed_start += 1;
            locked_rti.set_num_feds_proposed_start(num_feds_proposed_start);
            if timestamp > max_start_time {
                locked_rti.set_max_start_time(timestamp);
            }
        }
        println!(
            "num_feds_proposed_start = {}, number_of_enclaves = {}",
            num_feds_proposed_start, number_of_enclaves
        );
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
                    let locked_rti = _f_rti.read().unwrap();
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
            let locked_rti = _f_rti.read().unwrap();
            max_start_time = locked_rti.max_start_time();
        }
        let mut locked_start_time = start_time.lock().unwrap();
        locked_start_time.set_start_time(max_start_time + net_common::DELAY_START);
        // TODO: Consider swap_bytes_if_big_endian_int64()
        let start_time = locked_start_time.start_time();
        NetUtil::encode_int64(start_time, &mut start_time_buffer, 1);

        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::SendTimestamp,
            fed_id,
            &Tag::new(start_time, 0),
            start_time,
            TraceDirection::To,
        );
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            let stream = my_fed.stream().as_ref().unwrap();
            let bytes_written = NetUtil::write_to_socket(stream, &start_time_buffer, fed_id);
            if bytes_written < MSG_TYPE_TIMESTAMP_LENGTH {
                println!(
                    "Failed to send the starting time to federate_info {}.",
                    fed_id
                );
            }

            // Update state for the federate_info to indicate that the MSG_TYPE_Timestamp
            // message has been sent. That MSG_TYPE_Timestamp message grants time advance to
            // the federate_info to the start time.
            my_fed.enclave_mut().set_state(SchedulingNodeState::Granted);
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

    /**
     * Handle MSG_TYPE_RESIGN sent by a federate. This message is sent at the time of termination
     * after all shutdown events are processed on the federate.
     *
     * This function assumes the caller does not hold the mutex.
     *
     * @note At this point, the RTI might have outgoing messages to the federate. This
     * function thus first performs a shutdown on the socket, which sends an EOF. It then
     * waits for the remote socket to be closed before closing the socket itself.
     */
    fn handle_federate_resign(
        fed_id: u16,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        // Nothing more to do. Close the socket and exit.

        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveResign,
            fed_id,
            &Tag::forever_tag(),
            start_time_value,
            TraceDirection::From,
        );

        println!("FederateInfo {} has resigned.", fed_id);

        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            my_fed
                .enclave_mut()
                .set_state(SchedulingNodeState::NotConnected);
        }

        // Indicate that there will no further events from this federate_info.
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let my_fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            my_fed.enclave_mut().set_next_event(Tag::forever_tag());
        }

        // According to this: https://stackoverflow.com/questions/4160347/close-vs-shutdown-socket,
        // the close should happen when receiving a 0 length message from the other end.
        // Here, we just signal the other side that no further writes to the socket are
        // forthcoming, which should result in the other end getting a zero-length reception.
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let my_fed = &locked_rti.base().scheduling_nodes()[idx];
            my_fed
                .stream()
                .as_ref()
                .unwrap()
                .shutdown(Shutdown::Both)
                .unwrap();
        }

        // Wait for the federate to send an EOF or a socket error to occur.
        // Discard any incoming bytes. Normally, this read should return 0 because
        // the federate is resigning and should itself invoke shutdown.
        // TODO: Check we need the below two lines.
        // unsigned char buffer[10];
        // while (read(my_fed->socket, buffer, 10) > 0);

        // Check downstream federates to see whether they should now be granted a TAG.
        // To handle cycles, need to create a boolean array to keep
        // track of which upstream federates have been visited.
        let number_of_enclaves;
        {
            let locked_rti = _f_rti.read().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
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

        // TODO: Move the below tracing into proper position.
        {
            let mut locked_rti = _f_rti.write().unwrap();
            if locked_rti.base().tracing_enabled() {
                // No need for a mutex lock because all threads have exited.
                if Trace::stop_trace_locked(locked_rti.base_mut().trace(), start_time_value) == true
                {
                    println!("RTI trace file saved.");
                }
            }
        }
    }

    fn handle_timed_message(
        message_type: u8,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
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
        NetUtil::read_from_socket_fail_on_error(
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
        NetUtil::read_from_socket_fail_on_error(
            stream,
            &mut message_buffer,
            fed_id,
            "timed message",
        );
        // FIXME: Handle "as i32" properly.
        let bytes_read = bytes_to_read + header_size as i32;
        // Following only works for string messages.
        // println!("Message received by RTI: {}.", buffer + header_size);

        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveTaggedMsg,
            fed_id,
            &intended_tag,
            start_time_value,
            TraceDirection::From,
        );

        let completed;
        let next_event;
        {
            // Need to acquire the mutex lock to ensure that the thread handling
            // messages coming from the socket connected to the destination does not
            // issue a TAG before this message has been forwarded.
            let locked_rti = _f_rti.read().unwrap();

            // If the destination federate_info is no longer connected, issue a warning
            // and return.
            let idx: usize = federate_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            let enclave = fed.enclave();
            next_event = enclave.next_event();
            if enclave.state() == SchedulingNodeState::NotConnected {
                println!(
                    "RTI: Destination federate_info {} is no longer connected. Dropping message.",
                    federate_id
                );
                println!("Fed status: next_event ({}, {}), completed ({}, {}), last_granted ({}, {}), last_provisionally_granted ({}, {}).",
                        next_event.time() - start_time_value,
                        next_event.microstep(),
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

        // Need to make sure that the destination federate_info's thread has already
        // sent the starting MsgType::Timestamp message.
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = federate_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            while fed.enclave().state() == SchedulingNodeState::Pending {
                // Need to wait here.
                let (lock, condvar) = &*sent_start_time;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
            }
        }

        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::SendTaggedMsg,
            federate_id,
            &intended_tag,
            start_time_value,
            TraceDirection::To,
        );

        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = federate_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            // FIXME: Handle unwrap properly.
            let destination_stream = fed.stream().as_ref().unwrap();
            let mut result_buffer = vec![0 as u8; 1];
            result_buffer[0] = message_type;
            result_buffer = vec![result_buffer.clone(), header_buffer, message_buffer].concat();
            NetUtil::write_to_socket_fail_on_error(
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
            NetUtil::read_from_socket_fail_on_error(
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
                let locked_rti = _f_rti.read().unwrap();
                let idx: usize = federate_id.into();
                let fed = &locked_rti.base().scheduling_nodes()[idx];
                // FIXME: Handle unwrap properly.
                let destination_stream = fed.stream().as_ref().unwrap();
                NetUtil::write_to_socket_fail_on_error(
                    destination_stream,
                    &forward_buffer,
                    federate_id,
                    "message chunks",
                );
            }
        }

        // Record this in-transit message in federate_info's in-transit message queue.
        if Tag::lf_tag_compare(&completed, &intended_tag) < 0 {
            // Add a record of this message to the list of in-transit messages to this federate_info.
            InTransitMessageQueue::insert_if_no_match_tag(
                _f_rti.clone(),
                federate_id,
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

        // If the message tag is less than the most recently received NET from the federate,
        // then update the federate's next event tag to match the message tag.
        if Tag::lf_tag_compare(&intended_tag, &next_event) < 0 {
            Self::update_federate_next_event_tag_locked(
                _f_rti,
                federate_id,
                intended_tag,
                start_time_value,
                sent_start_time,
            );
        }
    }

    fn update_federate_next_event_tag_locked(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        mut next_event_tag: Tag,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let min_in_transit_tag = InTransitMessageQueue::peek_tag(_f_rti.clone(), fed_id);
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
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut header_buffer = vec![0 as u8; mem::size_of::<i64>() + mem::size_of::<u32>()];
        NetUtil::read_from_socket_fail_on_error(
            stream,
            &mut header_buffer,
            fed_id,
            "the content of the next event tag",
        );

        // Acquire a mutex lock to ensure that this state does not change while a
        // message is in transport or being used to determine a TAG.
        let enclave_id;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
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
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveNet,
            fed_id,
            &intended_tag,
            start_time_value,
            TraceDirection::From,
        );
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

    fn handle_latest_tag_complete(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let mut header_buffer = vec![0 as u8; mem::size_of::<i64>() + mem::size_of::<u32>()];
        NetUtil::read_from_socket_fail_on_error(
            stream,
            &mut header_buffer,
            fed_id,
            "the content of the latest tag complete",
        );
        let completed = NetUtil::extract_tag(
            header_buffer[0..(mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveLtc,
            fed_id,
            &completed,
            start_time_value,
            TraceDirection::From,
        );
        let number_of_enclaves;
        {
            let locked_rti = _f_rti.read().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        SchedulingNode::_logical_tag_complete(
            _f_rti.clone(),
            fed_id,
            number_of_enclaves,
            start_time_value,
            sent_start_time,
            completed.clone(),
        );

        // See if we can remove any of the recorded in-transit messages for this.
        InTransitMessageQueue::remove_up_to(_f_rti.clone(), fed_id, completed);
    }

    fn handle_stop_request_message(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        stop_granted: Arc<Mutex<StopGranted>>,
    ) {
        println!("RTI handling stop_request from federate_info {}.", fed_id);

        let mut header_buffer = vec![0 as u8; MSG_TYPE_STOP_REQUEST_LENGTH - 1];
        NetUtil::read_from_socket_fail_on_error(
            stream,
            &mut header_buffer,
            fed_id,
            "the MsgType::StopRequest payload",
        );

        // Extract the proposed stop tag for the federate_info
        let proposed_stop_tag = NetUtil::extract_tag(
            header_buffer[0..(mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveStopReq,
            fed_id,
            &proposed_stop_tag,
            start_time_value,
            TraceDirection::From,
        );

        println!(
            "RTI received from federate_info {} a MsgType::StopRequest message with tag ({},{}).",
            fed_id,
            proposed_stop_tag.time() - start_time_value,
            proposed_stop_tag.microstep()
        );

        // Acquire a mutex lock to ensure that this state does change while a
        // message is in transport or being used to determine a TAG.
        let requested_stop;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            requested_stop = fed.requested_stop();
        }
        // Check whether we have already received a stop_tag
        // from this federate_info
        if requested_stop {
            // If stop request messages have already been broadcast, treat this as if it were a reply.
            let stop_in_progress;
            {
                let locked_rti = _f_rti.read().unwrap();
                stop_in_progress = locked_rti.stop_in_progress();
            }
            if stop_in_progress {
                Self::mark_federate_requesting_stop(
                    fed_id,
                    _f_rti.clone(),
                    stop_granted.clone(),
                    start_time_value,
                );
            }
            return;
        }

        // Update the maximum stop tag received from federates
        {
            let mut locked_rti = _f_rti.write().unwrap();
            if Tag::lf_tag_compare(&proposed_stop_tag, &locked_rti.base().max_stop_tag()) > 0 {
                locked_rti
                    .base_mut()
                    .set_max_stop_tag(proposed_stop_tag.clone());
            }
        }

        // If all federates have replied, send stop request granted.
        if Self::mark_federate_requesting_stop(
            fed_id,
            _f_rti.clone(),
            stop_granted.clone(),
            start_time_value,
        ) {
            // Have send stop request granted to all federates. Nothing more to do.
            return;
        }

        // Forward the stop request to all other federates that have not
        // also issued a stop request.
        let mut stop_request_buffer = vec![0 as u8; MSG_TYPE_STOP_REQUEST_LENGTH];
        {
            let locked_rti = _f_rti.read().unwrap();
            Self::encode_stop_request(
                &mut stop_request_buffer,
                locked_rti.base().max_stop_tag().time(),
                locked_rti.base().max_stop_tag().microstep(),
            );
        }

        // Iterate over federates and send each the MSG_TYPE_StopRequest message
        // if we do not have a stop_time already for them. Do not do this more than once.
        {
            let mut locked_rti = _f_rti.write().unwrap();
            if locked_rti.stop_in_progress() {
                return;
            }
            locked_rti.set_stop_in_progress(true);
        }
        // Need a timeout here in case a federate never replies.
        // TODO: Implement 'wait_for_stop_request_reply' function.
        // lf_thread_create(&timeout_thread, wait_for_stop_request_reply, NULL);

        let number_of_enclaves;
        {
            let locked_rti = _f_rti.read().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        for i in 0..number_of_enclaves {
            let max_stop_tag;
            let enc_id;
            let requested_stop;
            let enc_state;
            {
                let locked_rti = _f_rti.read().unwrap();
                max_stop_tag = locked_rti.base().max_stop_tag();
                let f = &locked_rti.base().scheduling_nodes()[i as usize];
                enc_id = f.enclave().id();
                requested_stop = f.requested_stop();
                enc_state = f.enclave().state();
            }
            if enc_id != fed_id && requested_stop == false {
                if enc_state == SchedulingNodeState::NotConnected {
                    Self::mark_federate_requesting_stop(
                        enc_id,
                        _f_rti.clone(),
                        stop_granted.clone(),
                        start_time_value,
                    );
                    continue;
                }

                Trace::log_trace(
                    _f_rti.clone(),
                    TraceEvent::SendStopReq,
                    enc_id,
                    &max_stop_tag,
                    start_time_value,
                    TraceDirection::To,
                );

                {
                    let locked_rti = _f_rti.read().unwrap();
                    let f = &locked_rti.base().scheduling_nodes()[i as usize];
                    // FIXME: Handle unwrap properly.
                    let stream = f.stream().as_ref().unwrap();
                    NetUtil::write_to_socket_fail_on_error(
                        stream,
                        &stop_request_buffer,
                        f.enclave().id(),
                        "MsgType::StopRequest message",
                    );
                }
            }
        }
        {
            let locked_rti = _f_rti.read().unwrap();
            println!(
                "RTI forwarded to federates MsgType::StopRequest with tag ({}, {}).",
                locked_rti.base().max_stop_tag().time() - start_time_value,
                locked_rti.base().max_stop_tag().microstep()
            );
        }
    }

    /**
     * Mark a federate requesting stop. If the number of federates handling stop reaches the
     * NUM_OF_FEDERATES, broadcast MsgType::StopGranted to every federate.
     * @return true if stop time has been sent to all federates and false otherwise.
     */
    fn mark_federate_requesting_stop(
        fed_id: u16,
        _f_rti: Arc<RwLock<RTIRemote>>,
        stop_granted: Arc<Mutex<StopGranted>>,
        start_time_value: Instant,
    ) -> bool {
        let mut num_enclaves_handling_stop;
        {
            let locked_rti = _f_rti.read().unwrap();
            num_enclaves_handling_stop = locked_rti.base().num_scheduling_nodes_handling_stop();
        }
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            if !fed.requested_stop() {
                // Assume that the federate_info
                // has requested stop
                locked_rti
                    .base_mut()
                    .set_num_scheduling_nodes_handling_stop(num_enclaves_handling_stop + 1);
            }
        }
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            if !fed.requested_stop() {
                // Assume that the federate_info
                // has requested stop
                fed.set_requested_stop(true);
            }
        }
        let number_of_enclaves;
        {
            let locked_rti = _f_rti.read().unwrap();
            num_enclaves_handling_stop = locked_rti.base().num_scheduling_nodes_handling_stop();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        if num_enclaves_handling_stop == number_of_enclaves {
            // We now have information about the stop time of all
            // federates.
            Self::broadcast_stop_time_to_federates_locked(_f_rti, stop_granted, start_time_value);
            return true;
        }
        false
    }

    /**
     * Once the RTI has seen proposed tags from all connected federates,
     * it will broadcast a MSG_TYPE_StopGranted carrying the _RTI.max_stop_tag.
     * This function also checks the most recently received NET from
     * each federate_info and resets that be no greater than the _RTI.max_stop_tag.
     *
     * This function assumes the caller holds the _RTI.rti_mutex lock.
     */
    fn broadcast_stop_time_to_federates_locked(
        _f_rti: Arc<RwLock<RTIRemote>>,
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
            let locked_rti = _f_rti.read().unwrap();
            Self::encode_stop_granted(
                &mut outgoing_buffer,
                locked_rti.base().max_stop_tag().time(),
                locked_rti.base().max_stop_tag().microstep(),
            );
        }

        let number_of_enclaves;
        {
            let locked_rti = _f_rti.read().unwrap();
            number_of_enclaves = locked_rti.base().number_of_scheduling_nodes();
        }
        // Iterate over federates and send each the message.
        for i in 0..number_of_enclaves {
            let next_event;
            let max_stop_tag;
            let enc_id;
            {
                let locked_rti = _f_rti.read().unwrap();
                max_stop_tag = locked_rti.base().max_stop_tag();
                // FIXME: Handle usize properly.
                let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[i as usize];
                enc_id = fed.enclave().id();
                next_event = fed.enclave().next_event();
                if fed.enclave().state() == SchedulingNodeState::NotConnected {
                    continue;
                }
            }
            {
                let mut locked_rti = _f_rti.write().unwrap();
                let fed: &mut FederateInfo =
                    &mut locked_rti.base_mut().scheduling_nodes_mut()[i as usize];
                if Tag::lf_tag_compare(&next_event, &max_stop_tag) >= 0 {
                    // Need the next_event to be no greater than the stop tag.
                    fed.enclave_mut().set_next_event(max_stop_tag.clone());
                }
            }
            Trace::log_trace(
                _f_rti.clone(),
                TraceEvent::SendStopGrn,
                enc_id,
                &max_stop_tag,
                start_time_value,
                TraceDirection::To,
            );
            {
                let locked_rti = _f_rti.read().unwrap();
                let fed = &locked_rti.base().scheduling_nodes()[i as usize];
                // FIXME: Handle unwrap properly.
                let stream = fed.stream().as_ref().unwrap();
                NetUtil::write_to_socket_fail_on_error(
                    stream,
                    &outgoing_buffer,
                    enc_id,
                    "MsgType::StopGranted message",
                );
            }
        }

        {
            let locked_rti = _f_rti.read().unwrap();
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
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        stop_granted: Arc<Mutex<StopGranted>>,
    ) {
        let mut header_buffer = vec![0 as u8; MSG_TYPE_STOP_REQUEST_REPLY_LENGTH - 1];
        NetUtil::read_from_socket_fail_on_error(
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
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveStopReqRep,
            fed_id,
            &federate_stop_tag,
            start_time_value,
            TraceDirection::From,
        );

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
            let locked_rti = _f_rti.read().unwrap();
            max_stop_tag = locked_rti.base().max_stop_tag();
        }
        if Tag::lf_tag_compare(&federate_stop_tag, &max_stop_tag) > 0 {
            let mut locked_rti = _f_rti.write().unwrap();
            locked_rti.base_mut().set_max_stop_tag(federate_stop_tag);
        }
        Self::mark_federate_requesting_stop(
            fed_id,
            _f_rti.clone(),
            stop_granted.clone(),
            start_time_value,
        );
    }

    fn handle_address_query(
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
    ) {
        // Use buffer both for reading and constructing the reply.
        // The length is what is needed for the reply.
        let mut buffer = vec![0 as u8; mem::size_of::<u16>()];
        NetUtil::read_from_socket_fail_on_error(stream, &mut buffer, fed_id, "address query.");
        let remote_fed_id = u16::from_le_bytes(buffer.clone().try_into().unwrap());

        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveAdrQr,
            fed_id,
            &Tag::forever_tag(),
            start_time_value,
            TraceDirection::From,
        );

        println!(
            "RTI received address query from {} for {}.",
            fed_id, remote_fed_id
        );

        // NOTE: server_port initializes to -1, which means the RTI does not know
        // the port number because it has not yet received an MsgType::AddressAdvertisement message
        // from this federate. In that case, it will respond by sending -1.

        // Response message is also of type MsgType::AddressQuery.
        let mut response_message = vec![0 as u8; 1 + mem::size_of::<i32>()];
        response_message[0] = MsgType::AddressQuery.to_byte();

        // Encode the port number.
        let server_hostname;
        let server_port;
        let server_ip_addr;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = remote_fed_id.into();
            let remote_fed = &locked_rti.base().scheduling_nodes()[idx];
            server_hostname = remote_fed.server_hostname();
            server_port = remote_fed.server_port();
            server_ip_addr = remote_fed.server_ip_addr();
        }
        // Send the port number (which could be -1).
        NetUtil::encode_int32(server_port, &mut response_message, 1);
        NetUtil::write_to_socket_fail_on_error(stream, &response_message, fed_id, "port number");

        // Send the server IP address to federate.
        match server_ip_addr.ip() {
            IpAddr::V4(ip_addr) => {
                NetUtil::write_to_socket_fail_on_error(
                    stream,
                    &(ip_addr.octets().to_vec()),
                    fed_id,
                    "ip address",
                );
            }
            IpAddr::V6(_e) => {
                println!("IPv6 is not supported yet. Exit.");
                std::process::exit(1);
            }
        }

        println!(
            "Replied to address query from federate {} with address {}({}):{}.",
            fed_id, server_hostname, server_ip_addr, server_port
        );
    }

    fn handle_address_ad(
        federate_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
    ) {
        // Read the port number of the federate that can be used for physical
        // connections to other federates
        let mut buffer = vec![0 as u8; mem::size_of::<i32>()];
        NetUtil::read_from_socket_fail_on_error(stream, &mut buffer, federate_id, "port data");

        // FIXME: Update from_le_bytes if endian types should be considered.
        let server_port = i32::from_le_bytes(buffer.try_into().unwrap());

        assert!(server_port <= u16::MAX.into());

        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = federate_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            fed.set_server_port(server_port);
        }

        println!(
            "Received address advertisement with port {} from federate {}.",
            server_port, federate_id
        );
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceiveAdrAd,
            federate_id,
            &Tag::forever_tag(),
            start_time_value,
            TraceDirection::From,
        );
    }

    fn handle_port_absent_message(
        buffer: &Vec<u8>,
        fed_id: u16,
        stream: &mut TcpStream,
        _f_rti: Arc<RwLock<RTIRemote>>,
        start_time: Arc<Mutex<tag::StartTime>>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let message_size =
            mem::size_of::<u16>() * 2 + mem::size_of::<i64>() + mem::size_of::<u32>();

        let mut header_buffer = vec![0 as u8; message_size];
        NetUtil::read_from_socket_fail_on_error(
            stream,
            &mut header_buffer,
            fed_id,
            "port absent message",
        );

        let u16_size = mem::size_of::<u16>();
        // FIXME: Update from_le_bytes if endian types should be considered.
        let reactor_port_id = u16::from_le_bytes(header_buffer[0..(u16_size)].try_into().unwrap());
        let federate_id = u16::from_le_bytes(
            header_buffer[(u16_size)..(u16_size + u16_size)]
                .try_into()
                .unwrap(),
        );

        let start_idx = u16_size * 2;
        let tag = NetUtil::extract_tag(
            header_buffer[start_idx..(start_idx + mem::size_of::<i64>() + mem::size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        let start_time_value;
        {
            let locked_start_time = start_time.lock().unwrap();
            start_time_value = locked_start_time.start_time();
        }
        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::ReceivePortAbs,
            fed_id,
            &tag,
            start_time_value,
            TraceDirection::From,
        );

        // Need to acquire the mutex lock to ensure that the thread handling
        // messages coming from the socket connected to the destination does not
        // issue a TAG before this message has been forwarded.
        {
            let locked_rti = _f_rti.read().unwrap();

            // If the destination federate_info is no longer connected, issue a warning
            // and return.
            let idx: usize = federate_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
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
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = federate_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            while fed.enclave().state() == SchedulingNodeState::Pending {
                // Need to wait here.
                let (lock, condvar) = &*sent_start_time;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
            }
        }

        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::SendPTag,
            fed_id,
            &tag,
            start_time_value,
            TraceDirection::To,
        );

        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = federate_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            // Forward the message.
            let destination_stream = fed.stream().as_ref().unwrap();
            let mut result_buffer = vec![0 as u8];
            result_buffer[0] = buffer[0];
            result_buffer = vec![result_buffer.clone(), header_buffer].concat();
            NetUtil::write_to_socket_fail_on_error(
                destination_stream,
                &result_buffer,
                federate_id,
                "message",
            );
        }
    }

    fn send_physical_clock_with_udp(fed_id: u16, _f_rti: Arc<RwLock<RTIRemote>>, message_type: u8) {
        let state;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            state = fed.enclave().state();
        }
        if state == SchedulingNodeState::NotConnected {
            println!("[WARNING] Clock sync: RTI failed to send physical time to federate {}. Socket not connected.\n",
                            fed_id);
            return;
        }
        let mut buffer = vec![0 as u8; std::mem::size_of::<i64>() + 1];
        buffer[0] = message_type;
        let current_physical_time = Tag::lf_time_physical();
        NetUtil::encode_int64(current_physical_time, &mut buffer, 1);

        // Send the message
        println!(
            "[DEBUG] Clock sync: RTI sending UDP message type {}.",
            buffer[0]
        );
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            // FIXME: udp_addr is initialized as 0.0.0.0.
            let udp_addr = fed.udp_addr();
            let socket = locked_rti.socket_descriptor_udp().as_mut().unwrap();
            match socket.send_to(&buffer, udp_addr) {
                Ok(bytes_written) => {
                    if bytes_written < 1 + std::mem::size_of::<i64>() {
                        println!("[WARNING] Clock sync: RTI failed to send physical time to federate {}: \n", fed_id);
                        return;
                    }
                }
                Err(_) => {
                    println!("Failed to send an UDP message.");
                    return;
                }
            }
        }
        println!("[DEBUG] Clock sync: RTI sent PHYSICAL_TIME_SYNC_MESSAGE with timestamp ({}) to federate {}.",
                    current_physical_time, fed_id);
    }

    fn handle_physical_clock_sync_message_with_udp(fed_id: u16, _f_rti: Arc<RwLock<RTIRemote>>) {
        // Reply with a T4 type message
        Self::send_physical_clock_with_udp(fed_id, _f_rti.clone(), MsgType::ClockSyncT4.to_byte());
        // Send the corresponding coded probe immediately after,
        // but only if this is a UDP channel.
        Self::send_physical_clock_with_udp(
            fed_id,
            _f_rti.clone(),
            MsgType::ClockSyncCodedProbe.to_byte(),
        );
    }

    fn send_physical_clock_with_tcp(
        fed_id: u16,
        _f_rti: Arc<RwLock<RTIRemote>>,
        message_type: u8,
        stream: &mut TcpStream,
    ) {
        let state;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            state = fed.enclave().state();
        }
        if state == SchedulingNodeState::NotConnected {
            println!("[WARNING] Clock sync: RTI failed to send physical time to federate {}. Socket not connected.\n",
                            fed_id);
            return;
        }
        let mut buffer = vec![0 as u8; std::mem::size_of::<i64>() + 1];
        buffer[0] = message_type;
        let current_physical_time = Tag::lf_time_physical();
        NetUtil::encode_int64(current_physical_time, &mut buffer, 1);

        // Send the message
        println!(
            "[DEBUG] Clock sync:  RTI sending TCP message type {}.",
            buffer[0]
        );
        NetUtil::write_to_socket_fail_on_error(stream, &buffer, fed_id, "physical time");
        println!("[DEBUG] Clock sync: RTI sent PHYSICAL_TIME_SYNC_MESSAGE with timestamp ({}) to federate {}.",
                    current_physical_time, fed_id);
    }

    fn handle_physical_clock_sync_message_with_tcp(
        fed_id: u16,
        _f_rti: Arc<RwLock<RTIRemote>>,
        stream: &mut TcpStream,
    ) {
        // Reply with a T4 type message
        Self::send_physical_clock_with_tcp(
            fed_id,
            _f_rti.clone(),
            MsgType::ClockSyncT4.to_byte(),
            stream,
        );
    }
}
