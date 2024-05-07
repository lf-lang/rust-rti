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
 * This file extends enclave.h with RTI features that are specific to federations and are not
 * used by scheduling enclaves.
 */
use crate::constants::*;
use crate::net_common::MsgType;
use crate::net_util::NetUtil;
use crate::tag::{Instant, Tag};
use crate::trace::{TraceDirection, TraceEvent};
use crate::ClockSyncStat;
use crate::FederateInfo;
use crate::RTICommon;
use crate::SchedulingNode;
use crate::SchedulingNodeState;
use crate::SchedulingNodeState::NotConnected;
use crate::Trace;

use std::io::Write;
use std::mem;
use std::net::UdpSocket;
use std::sync::{Arc, Condvar, Mutex, RwLock};

/**
 * Structure that an RTI instance uses to keep track of its own and its
 * corresponding federates' state.
 */
pub struct RTIRemote {
    base: RTICommon,

    // Maximum start time seen so far from the federates.
    max_start_time: i64,

    // Number of federates that have proposed start times.
    num_feds_proposed_start: i32,

    /**
     * Boolean indicating that all federates have exited.
     * This gets set to true exactly once before the program exits.
     * It is marked volatile because the write is not guarded by a mutex.
     * The main thread makes this true, then calls shutdown and close on
     * the socket, which will cause accept() to return with an error code
     * in respond_to_erroneous_connections().
     */
    // TODO: volatile bool all_federates_exited;

    /**
     * The ID of the federation that this RTI will supervise.
     * This should be overridden with a command-line -i option to ensure
     * that each federate_info only joins its assigned federation.
     */
    federation_id: String,

    /************* TCP server information *************/
    /** The desired port specified by the user on the command line. */
    user_specified_port: u16,

    /** The final port number that the TCP socket server ends up using. */
    final_port_tcp: u16,

    /** The TCP socket descriptor for the socket server. */
    socket_descriptor_tcp: i32,

    /************* UDP server information *************/
    /** The final port number that the UDP socket server ends up using. */
    final_port_udp: u16,

    /** The UDP socket descriptor for the socket server. */
    socket_descriptor_udp: Option<UdpSocket>,

    /************* Clock synchronization information *************/
    /* Thread performing PTP clock sync sessions periodically. */
    // TODO: lf_thread_t clock_thread;
    /**
     * Indicates whether clock sync is globally on for the federation. Federates
     * can still selectively disable clock synchronization if they wanted to.
     */
    clock_sync_global_status: ClockSyncStat,

    /**
     * Frequency (period in nanoseconds) between clock sync attempts.
     */
    clock_sync_period_ns: u64,

    /**
     * Number of messages exchanged for each clock sync attempt.
     */
    clock_sync_exchanges_per_interval: u32,

    /**
     * Boolean indicating that authentication is enabled.
     */
    authentication_enabled: bool,

    /**
     * Boolean indicating that a stop request is already in progress.
     */
    stop_in_progress: bool,
}

impl RTIRemote {
    pub fn new() -> RTIRemote {
        RTIRemote {
            base: RTICommon::new(),
            max_start_time: 0,
            num_feds_proposed_start: 0,
            // all_federates_exited:false,
            federation_id: String::from("Unidentified Federation"),
            user_specified_port: STARTING_PORT,
            final_port_tcp: 0,
            socket_descriptor_tcp: -1,
            final_port_udp: u16::MAX,
            socket_descriptor_udp: None,
            clock_sync_global_status: ClockSyncStat::ClockSyncInit,
            clock_sync_period_ns: 10 * 1000000,
            clock_sync_exchanges_per_interval: 10,
            authentication_enabled: false,
            stop_in_progress: false,
        }
    }

    pub fn base(&self) -> &RTICommon {
        &self.base
    }

    pub fn base_mut(&mut self) -> &mut RTICommon {
        &mut self.base
    }

    pub fn max_start_time(&self) -> i64 {
        self.max_start_time
    }

    pub fn num_feds_proposed_start(&self) -> i32 {
        self.num_feds_proposed_start
    }

    pub fn federation_id(&self) -> String {
        self.federation_id.clone()
    }

    pub fn user_specified_port(&self) -> u16 {
        self.user_specified_port
    }

    pub fn final_port_tcp(&self) -> u16 {
        self.final_port_tcp
    }

    pub fn socket_descriptor_udp(&mut self) -> &mut Option<UdpSocket> {
        &mut self.socket_descriptor_udp
    }

    pub fn final_port_udp(&self) -> u16 {
        self.final_port_udp
    }

    pub fn clock_sync_global_status(&self) -> ClockSyncStat {
        self.clock_sync_global_status.clone()
    }

    pub fn clock_sync_period_ns(&self) -> u64 {
        self.clock_sync_period_ns
    }

    pub fn clock_sync_exchanges_per_interval(&self) -> u32 {
        self.clock_sync_exchanges_per_interval
    }

    pub fn stop_in_progress(&self) -> bool {
        self.stop_in_progress
    }

    pub fn set_max_start_time(&mut self, max_start_time: i64) {
        self.max_start_time = max_start_time;
    }

    pub fn set_num_feds_proposed_start(&mut self, num_feds_proposed_start: i32) {
        self.num_feds_proposed_start = num_feds_proposed_start;
    }

    pub fn set_federation_id(&mut self, federation_id: String) {
        self.federation_id = federation_id;
    }

    // set_user_specified_port
    pub fn set_port(&mut self, user_specified_port: u16) {
        self.user_specified_port = user_specified_port;
    }

    pub fn set_final_port_tcp(&mut self, final_port_tcp: u16) {
        self.final_port_tcp = final_port_tcp;
    }

    pub fn set_socket_descriptor_udp(&mut self, socket_descriptor_udp: Option<UdpSocket>) {
        self.socket_descriptor_udp = socket_descriptor_udp;
    }

    pub fn set_final_port_udp(&mut self, final_port_udp: u16) {
        self.final_port_udp = final_port_udp;
    }

    pub fn set_clock_sync_global_status(&mut self, clock_sync_global_status: ClockSyncStat) {
        self.clock_sync_global_status = clock_sync_global_status;
    }

    pub fn set_clock_sync_period_ns(&mut self, clock_sync_period_ns: u64) {
        self.clock_sync_period_ns = clock_sync_period_ns;
    }

    pub fn set_clock_sync_exchanges_per_interval(
        &mut self,
        clock_sync_exchanges_per_interval: u32,
    ) {
        self.clock_sync_exchanges_per_interval = clock_sync_exchanges_per_interval;
    }

    pub fn set_stop_in_progress(&mut self, stop_in_progress: bool) {
        self.stop_in_progress = stop_in_progress;
    }

    /**
     * Notify a tag advance grant (TAG) message to the specified scheduling node.
     * Do not notify it if a previously sent PTAG was greater or if a
     * previously sent TAG was greater or equal.
     *
     * This function will keep a record of this TAG in the node's last_granted
     * field.
     *
     * This function assumes that the caller holds the RTI mutex.
     */
    pub fn notify_tag_advance_grant(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        tag: Tag,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            let e = fed.enclave();
            if e.state() == SchedulingNodeState::NotConnected
                || Tag::lf_tag_compare(&tag, &e.last_granted()) <= 0
                || Tag::lf_tag_compare(&tag, &e.last_provisionally_granted()) <= 0
            {
                return;
            }
            // Need to make sure that the destination federate's thread has already
            // sent the starting MSG_TYPE_TIMESTAMP message.
            while e.state() == SchedulingNodeState::Pending {
                // Need to wait here.
                let (lock, condvar) = &*sent_start_time;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
            }
        }
        let message_length = 1 + mem::size_of::<i64>() + mem::size_of::<u32>();
        let mut buffer = vec![0 as u8; message_length as usize];
        buffer[0] = MsgType::TagAdvanceGrant.to_byte();
        NetUtil::encode_int64(tag.time(), &mut buffer, 1);
        // FIXME: Replace "as i32" properly.
        NetUtil::encode_int32(
            tag.microstep() as i32,
            &mut buffer,
            1 + mem::size_of::<i64>(),
        );

        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::SendTag,
            fed_id,
            &tag,
            start_time,
            TraceDirection::To,
        );
        // This function is called in notify_advance_grant_if_safe(), which is a long
        // function. During this call, the socket might close, causing the following write_to_socket
        // to fail. Consider a failure here a soft failure and update the federate's status.
        let mut error_occurred = false;
        {
            let locked_rti = _f_rti.read().unwrap();
            let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[fed_id as usize];
            let e = fed.enclave();
            let mut stream = fed.stream().as_ref().unwrap();
            match stream.write(&buffer) {
                Ok(bytes_written) => {
                    if bytes_written < message_length {
                        println!(
                            "RTI failed to send tag advance grant to federate {}.",
                            e.id()
                        );
                    }
                }
                Err(_err) => {
                    error_occurred = true;
                }
            }
        }
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let mut_fed: &mut FederateInfo =
                &mut locked_rti.base_mut().scheduling_nodes_mut()[fed_id as usize];
            let enclave = mut_fed.enclave_mut();
            if error_occurred {
                enclave.set_state(SchedulingNodeState::NotConnected);
                // FIXME: We need better error handling, but don't stop other execution here.
            } else {
                enclave.set_last_granted(tag.clone());
                println!(
                    "RTI sent to federate {} the Tag Advance Grant (TAG) ({},{}).",
                    enclave.id(),
                    tag.time() - start_time,
                    tag.microstep()
                );
            }
        }
    }

    /**
     * Notify a provisional tag advance grant (PTAG) message to the specified scheduling node.
     * Do not notify it if a previously sent PTAG or TAG was greater or equal.
     *
     * This function will keep a record of this PTAG in the node's last_provisionally_granted
     * field.
     *
     * This function assumes that the caller holds the RTI mutex.
     */
    pub fn notify_provisional_tag_advance_grant(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        number_of_enclaves: i32,
        tag: Tag,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[idx];
            let e = fed.enclave();
            if e.state() == SchedulingNodeState::NotConnected
                || Tag::lf_tag_compare(&tag, &e.last_granted()) <= 0
                || Tag::lf_tag_compare(&tag, &e.last_provisionally_granted()) <= 0
            {
                return;
            }
            // Need to make sure that the destination federate's thread has already
            // sent the starting MSG_TYPE_TIMESTAMP message.
            while e.state() == SchedulingNodeState::Pending {
                // Need to wait here.
                let (lock, condvar) = &*sent_start_time;
                let mut notified = lock.lock().unwrap();
                while !*notified {
                    notified = condvar.wait(notified).unwrap();
                }
            }
        }
        let message_length = 1 + mem::size_of::<i64>() + mem::size_of::<u32>();
        let mut buffer = vec![0 as u8; message_length as usize];
        buffer[0] = MsgType::PropositionalTagAdvanceGrant.to_byte();
        NetUtil::encode_int64(tag.time(), &mut buffer, 1);
        // FIXME: Handle "as i32" properly.
        NetUtil::encode_int32(
            tag.microstep() as i32,
            &mut buffer,
            1 + mem::size_of::<i64>(),
        );

        Trace::log_trace(
            _f_rti.clone(),
            TraceEvent::SendPTag,
            fed_id,
            &tag,
            start_time,
            TraceDirection::To,
        );
        // This function is called in notify_advance_grant_if_safe(), which is a long
        // function. During this call, the socket might close, causing the following write_to_socket
        // to fail. Consider a failure here a soft failure and update the federate's status.
        let mut error_occurred = false;
        {
            let locked_rti = _f_rti.read().unwrap();
            let enclaves = locked_rti.base().scheduling_nodes();
            let fed: &FederateInfo = &enclaves[fed_id as usize];
            let e = fed.enclave();
            let mut stream = fed.stream().as_ref().unwrap();
            match stream.write(&buffer) {
                Ok(bytes_written) => {
                    if bytes_written < message_length {
                        println!(
                            "RTI failed to send tag advance grant to federate {}.",
                            e.id()
                        );
                        return;
                    }
                }
                Err(_err) => {
                    error_occurred = true;
                }
            }
        }
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let mut_fed: &mut FederateInfo =
                &mut locked_rti.base_mut().scheduling_nodes_mut()[fed_id as usize];
            let enclave = mut_fed.enclave_mut();
            if error_occurred {
                enclave.set_state(SchedulingNodeState::NotConnected);
                // FIXME: We need better error handling, but don't stop other execution here.
            }

            enclave.set_last_provisionally_granted(tag.clone());
            println!(
                "RTI sent to federate {} the Provisional Tag Advance Grant (PTAG) ({},{}).",
                enclave.id(),
                tag.time() - start_time,
                tag.microstep()
            );
        }

        // Send PTAG to all upstream federates, if they have not had
        // a later or equal PTAG or TAG sent previously and if their transitive
        // NET is greater than or equal to the tag.
        // NOTE: This could later be replaced with a TNET mechanism once
        // we have an available encoding of causality interfaces.
        // That might be more efficient.
        // NOTE: This is not needed for enclaves because zero-delay loops are prohibited.
        // It's only needed for federates, which is why this is implemented here.
        let num_upstream;
        {
            let locked_rti = _f_rti.read().unwrap();
            let enclaves = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            let fed: &FederateInfo = &enclaves[idx];
            let e = fed.enclave();
            num_upstream = e.num_upstream();
        }
        for j in 0..num_upstream {
            let e_id;
            {
                let locked_rti = _f_rti.read().unwrap();
                let enclaves = locked_rti.base().scheduling_nodes();
                let idx: usize = fed_id.into();
                let fed: &FederateInfo = &enclaves[idx];
                e_id = fed.enclave().upstream()[j as usize];
                let upstream: &FederateInfo = &enclaves[e_id as usize];

                // Ignore this federate if it has resigned.
                if upstream.enclave().state() == NotConnected {
                    continue;
                }
            }
            // FIXME: Replace "as u16" properly.
            let earlist = SchedulingNode::earliest_future_incoming_message_tag(
                _f_rti.clone(),
                e_id as u16,
                start_time,
            );

            // If these tags are equal, then a TAG or PTAG should have already been granted,
            // in which case, another will not be sent. But it may not have been already granted.
            if Tag::lf_tag_compare(&earlist, &tag) >= 0 {
                Self::notify_provisional_tag_advance_grant(
                    _f_rti.clone(),
                    // FIXME: Replace "as u16" properly.
                    e_id as u16,
                    number_of_enclaves,
                    tag.clone(),
                    start_time,
                    sent_start_time.clone(),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::initialize_federates;
    use crate::initialize_rti;
    use crate::process_args;

    use rand::distributions::Alphanumeric;
    use rand::Rng;

    const FEDERATION_ID_MAX_SIZE: usize = 256;
    const RUST_RTI_PROGRAM_PATH: &str = "target/debug/rti";
    const RUST_RTI_NUMBER_OF_FEDERATES_OPTION: &str = "-n";
    const NUMBER_OF_FEDEATES: i32 = 2;

    #[test]
    // TODO: Better tp seperate each assert into a unit test, respectively.
    fn test_rti_remote_positive() {
        let rti_remote = RTIRemote::new();
        assert!(rti_remote.max_start_time() == 0);
        assert!(rti_remote.num_feds_proposed_start() == 0);
        assert!(rti_remote.federation_id() == "Unidentified Federation");
        assert!(rti_remote.user_specified_port() == STARTING_PORT);
        assert!(rti_remote.final_port_udp() == u16::MAX);
        assert!(rti_remote.clock_sync_global_status() == ClockSyncStat::ClockSyncInit);
        assert!(rti_remote.stop_in_progress() == false);
    }

    #[test]
    fn test_set_max_start_time_positive() {
        let mut rti_remote = RTIRemote::new();
        let mut rng = rand::thread_rng();
        let max_start_time: i64 = rng.gen_range(0..i64::MAX);
        rti_remote.set_max_start_time(max_start_time);
        assert!(rti_remote.max_start_time() == max_start_time);
    }

    #[test]
    fn test_set_num_feds_proposed_start_positive() {
        let mut rti_remote = RTIRemote::new();
        let mut rng = rand::thread_rng();
        let num_feds_proposed_start: i32 = rng.gen_range(0..i32::MAX);
        rti_remote.set_num_feds_proposed_start(num_feds_proposed_start);
        assert!(rti_remote.num_feds_proposed_start() == num_feds_proposed_start);
    }

    #[test]
    fn test_set_federation_id_positive() {
        let mut rti_remote = RTIRemote::new();
        let federation_id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(FEDERATION_ID_MAX_SIZE)
            .map(char::from)
            .collect();
        rti_remote.set_federation_id(federation_id.clone());
        assert!(rti_remote.federation_id() == federation_id);
    }

    #[test]
    fn test_set_user_specified_port_positive() {
        let mut rti_remote = RTIRemote::new();
        let mut rng = rand::thread_rng();
        let user_specified_port: u16 = rng.gen_range(0..u16::MAX);
        rti_remote.set_port(user_specified_port);
        assert!(rti_remote.user_specified_port() == user_specified_port);
    }

    #[test]
    fn test_set_stop_in_progress_positive() {
        let mut rti_remote = RTIRemote::new();
        rti_remote.set_stop_in_progress(true);
        assert!(rti_remote.stop_in_progress() == true);
    }

    #[test]
    fn test_notify_tag_advance_grant_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(RUST_RTI_PROGRAM_PATH.to_string());
        args.push(RUST_RTI_NUMBER_OF_FEDERATES_OPTION.to_string());
        args.push(NUMBER_OF_FEDEATES.to_string());
        let _ = process_args(&mut rti, &args);
        initialize_federates(&mut rti);
        let arc_rti = Arc::new(RwLock::new(rti));
        let cloned_rti = Arc::clone(&arc_rti);
        let sent_start_time = Arc::new((Mutex::new(false), Condvar::new()));
        let cloned_sent_start_time = Arc::clone(&sent_start_time);
        RTIRemote::notify_tag_advance_grant(
            cloned_rti,
            0,
            Tag::new(0, 0),
            0,
            cloned_sent_start_time,
        );
    }

    #[test]
    fn test_notify_provisional_tag_advance_grant_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(RUST_RTI_PROGRAM_PATH.to_string());
        args.push(RUST_RTI_NUMBER_OF_FEDERATES_OPTION.to_string());
        args.push(NUMBER_OF_FEDEATES.to_string());
        let _ = process_args(&mut rti, &args);
        initialize_federates(&mut rti);
        let arc_rti = Arc::new(RwLock::new(rti));
        let cloned_rti = Arc::clone(&arc_rti);
        let sent_start_time = Arc::new((Mutex::new(false), Condvar::new()));
        let cloned_sent_start_time = Arc::clone(&sent_start_time);
        RTIRemote::notify_provisional_tag_advance_grant(
            cloned_rti,
            0,
            NUMBER_OF_FEDEATES,
            Tag::new(0, 0),
            0,
            cloned_sent_start_time,
        );
    }
}
