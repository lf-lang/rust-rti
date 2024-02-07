/**
 * @file
 * @author Edward A. Lee (eal@berkeley.edu)
 * @author Soroush Bateni (soroush@utdallas.edu)
 * @author Erling Jellum (erling.r.jellum@ntnu.no)
 * @author Chadlia Jerad (chadlia.jerad@ensi-uma.tn)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @author Hokeun Kim (hokeun@asu.edu)
 * @copyright (c) 2020-2024, The University of California at Berkeley
 * License in [BSD 2-clause](https://github.com/lf-lang/reactor-c/blob/main/LICENSE.md)
 */
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};

use std::option::Option;

use crate::in_transit_message_queue::InTransitMessageQueue;
use crate::SchedulingNode;

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
    // TODO: lf_thread_t thread_id;
    stream: Option<TcpStream>, // The TCP socket descriptor for communicating with this federate.
    // TODO: struct sockaddr_in UDP_addr;
    clock_synchronization_enabled: bool, // Indicates the status of clock synchronization
    // for this federate. Enabled by default.
    in_transit_message_tags: InTransitMessageQueue, // Record of in-transit messages to this federate that are not
    // yet processed. This record is ordered based on the time
    // value of each message for a more efficient access.
    server_hostname: String, // Human-readable IP address and
    server_port: i32,        // port number of the socket server of the federate
    // if it has any incoming direct connections from other federates.
    // The port number will be -1 if there is no server or if the
    // RTI has not been informed of the port number.
    // TODO: struct in_addr server_ip_addr;
    // server of the federate.
    server_ip_addr: SocketAddr, // Information about the IP address of the socket
                                // server of the federate.
}

impl FederateInfo {
    pub fn new() -> FederateInfo {
        FederateInfo {
            enclave: SchedulingNode::new(),
            requested_stop: false,
            stream: None::<TcpStream>,
            clock_synchronization_enabled: true,
            in_transit_message_tags: InTransitMessageQueue::new(),
            server_hostname: String::from("localhost"),
            server_port: -1,
            server_ip_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        }
    }

    pub fn enclave(&self) -> &SchedulingNode {
        &self.enclave
    }

    pub fn enclave_mut(&mut self) -> &mut SchedulingNode {
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

    pub fn in_transit_message_queue(&self) -> &InTransitMessageQueue {
        &self.in_transit_message_tags
    }

    pub fn in_transit_message_queue_mut(&mut self) -> &mut InTransitMessageQueue {
        &mut self.in_transit_message_tags
    }

    pub fn server_hostname(&self) -> String {
        self.server_hostname.clone()
    }

    pub fn server_port(&self) -> i32 {
        self.server_port
    }

    pub fn server_ip_addr(&self) -> SocketAddr {
        self.server_ip_addr.clone()
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

    pub fn set_server_hostname(&mut self, server_hostname: String) {
        self.server_hostname = server_hostname;
    }

    pub fn set_server_port(&mut self, server_port: i32) {
        self.server_port = server_port;
    }

    pub fn set_server_ip_addr(&mut self, server_ip_addr: SocketAddr) {
        self.server_ip_addr = server_ip_addr;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use std::net::TcpStream;

    #[test]
    fn test_new_positive() {
        let _federate_info = FederateInfo::new();
        // TODO: Check federate_info
        assert!(true);
    }

    #[test]
    fn test_e_positive() {
        let federate_info = FederateInfo::new();
        let e = federate_info.e();
        assert!(e == &SchedulingNode::new());
    }

    #[test]
    fn test_enclave_positive() {
        let mut federate_info = FederateInfo::new();
        let enclave = federate_info.enclave();
        assert!(enclave == &SchedulingNode::new());
    }

    #[test]
    fn test_requested_stop_positive() {
        let federate_info = FederateInfo::new();
        let is_requested_stop = federate_info.requested_stop();
        assert!(is_requested_stop == false);
    }

    #[test]
    fn test_initial_stream_positive() {
        let federate_info = FederateInfo::new();
        let _initial_stream = federate_info.stream();
        // TODO: Check initial_stream
        assert!(true);
    }

    #[test]
    fn test_clock_synchronization_enabled_positive() {
        let federate_info = FederateInfo::new();
        let is_clock_synchronization_enabled = federate_info.clock_synchronization_enabled();
        assert!(is_clock_synchronization_enabled == true);
    }

    #[test]
    fn test_in_transit_message_tags_positive() {
        let mut federate_info = FederateInfo::new();
        let in_transit_message_tags = federate_info.in_transit_message_tags();
        assert!(in_transit_message_tags == &mut InTransitMessageRecordQueue::new())
    }

    #[test]
    fn test_set_requested_stop_as_true_positive() {
        let mut federate_info = FederateInfo::new();
        federate_info.set_requested_stop(true);
        assert!(federate_info.requested_stop() == true);
    }

    #[test]
    fn test_set_requested_stop_as_false_positive() {
        let mut federate_info = FederateInfo::new();
        federate_info.set_requested_stop(false);
        assert!(federate_info.requested_stop() == false);
    }

    #[test]
    fn test_set_stream_with_valid_stream_positive() {
        // TODO: Enable below
        // let mut federate_info = FederateInfo::new();
        // match TcpStream::connect("127.0.0.1:8080") {
        //     Ok(valid_stream) => {
        //         federate_info.set_stream(valid_stream);
        //         assert!(federate_info.stream() == valid_stream);
        //     }
        //     Err(_e) => {
        //         assert!(false);
        //     }
        // };
    }

    #[test]
    fn test_set_clock_synchronization_enabled_with_true_positive() {
        let mut federate_info = FederateInfo::new();
        federate_info.set_clock_synchronization_enabled(true);
        assert!(federate_info.clock_synchronization_enabled() == true);
    }

    #[test]
    fn test_set_clock_synchronization_enabled_with_false_positive() {
        let mut federate_info = FederateInfo::new();
        federate_info.set_clock_synchronization_enabled(false);
        assert!(federate_info.clock_synchronization_enabled() == false);
    }

    #[test]
    fn test_set_server_hostname_with_valid_name_positive() {
        let mut federate_info = FederateInfo::new();
        let server_name = String::from("test_rust_rti_server");
        federate_info.set_server_hostname(server_name.clone());
        assert!(federate_info.server_hostname() == server_name);
    }

    #[test]
    fn test_set_server_hostname_with_invalid_name_negative() {
        let mut federate_info = FederateInfo::new();
        let server_name = String::from("");
        federate_info.set_server_hostname(server_name.clone());
        assert!(federate_info.server_hostname().len() == 0);
    }

    #[test]
    fn test_set_server_port_with_valid_port_positive() {
        let mut federate_info = FederateInfo::new();
        let server_port = 8080;
        federate_info.set_server_port(server_port);
        assert!(federate_info.server_port() == server_port);
    }

    #[test]
    fn test_set_server_port_with_invalid_port_negative() {
        let mut federate_info = FederateInfo::new();
        let server_port = -1;
        federate_info.set_server_port(server_port);
        assert!(federate_info.server_port() < 0);
    }
}
