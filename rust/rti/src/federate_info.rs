use crate::message_record::message_record::InTransitMessageRecordQueue;
/**
 * @file
 * @author Edward A. Lee (eal@berkeley.edu)
 * @author Soroush Bateni (soroush@utdallas.edu)
 * @author Erling Jellum (erling.r.jellum@ntnu.no)
 * @author Chadlia Jerad (chadlia.jerad@ensi-uma.tn)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @author Hokeun Kim (hokeun@asu.edu)
 * @copyright (c) 2020-2023, The University of California at Berkeley
 * License in [BSD 2-clause](..)
 * @brief Declarations for runtime infrastructure (RTI) for distributed Lingua Franca programs.
 * This file extends rti_common.h with RTI features that are specific to federations and are not
 * used by scheduling enclaves.
 */
use crate::rti_common::*;

use std::net::TcpStream;
use std::option::Option;

/**
 * Information about a federate known to the RTI, including its runtime state,
 * mode of execution, and connectivity with other federates.
 * The list of upstream and downstream federates does not include
 * those that are connected via a "physical" connection (one
 * denoted with ~>) because those connections do not impose
 * any scheduling constraints.
 */
pub struct FederateInfo {
    enclave: SchedulingNode,
    requested_stop: bool, // Indicates that the federate has requested stop or has replied
    // to a request for stop from the RTI. Used to prevent double-counting
    // a federate when handling lf_request_stop().
    // TODO: lf_thread_t thread_id;    // The ID of the thread handling communication with this federate.
    stream: Option<TcpStream>, // The TCP socket descriptor for communicating with this federate.
    // TODO: struct sockaddr_in UDP_addr;           // The UDP address for the federate.
    clock_synchronization_enabled: bool, // Indicates the status of clock synchronization
    // for this federate. Enabled by default.
    in_transit_message_tags: InTransitMessageRecordQueue, // Record of in-transit messages to this federate that are not
    // yet processed. This record is ordered based on the time
    // value of each message for a more efficient access.
    server_hostname: String, // Human-readable IP address and
    server_port: i32,        // port number of the socket server of the federate
                             // if it has any incoming direct connections from other federates.
                             // The port number will be -1 if there is no server or if the
                             // RTI has not been informed of the port number.
                             // TODO: struct in_addr server_ip_addr; // Information about the IP address of the socket
                             // server of the federate.
}

impl FederateInfo {
    pub fn new() -> FederateInfo {
        FederateInfo {
            enclave: SchedulingNode::new(),
            requested_stop: false,
            stream: None::<TcpStream>,
            clock_synchronization_enabled: true,
            in_transit_message_tags: InTransitMessageRecordQueue::new(),
            server_hostname: String::from("localhost"),
            server_port: -1,
        }
    }

    pub fn e(&self) -> &SchedulingNode {
        &self.enclave
    }

    pub fn enclave(&mut self) -> &mut SchedulingNode {
        &mut self.enclave
    }

    pub fn requested_stop(&self) -> bool {
        self.requested_stop
    }

    pub fn stream(&self) -> &Option<TcpStream> {
        &self.stream
    }

    pub fn clock_synchronization_enabled(&self) -> bool {
        self.clock_synchronization_enabled
    }

    pub fn set_requested_stop(&mut self, requested_stop: bool) {
        self.requested_stop = requested_stop;
    }

    pub fn set_stream(&mut self, stream: TcpStream) {
        self.stream = Some(stream);
    }

    pub fn set_clock_synchronization_enabled(&mut self, clock_synchronization_enabled: bool) {
        self.clock_synchronization_enabled = clock_synchronization_enabled;
    }

    pub fn in_transit_message_tags(&mut self) -> &mut InTransitMessageRecordQueue {
        &mut self.in_transit_message_tags
    }

    pub fn set_server_hostname(&mut self, server_hostname: String) {
        self.server_hostname = server_hostname;
    }

    pub fn set_server_port(&mut self, server_port: i32) {
        self.server_port = server_port;
    }
}
