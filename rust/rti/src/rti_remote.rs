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
use crate::ClockSyncStat;
use crate::RTICommon;

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
    socket_descriptor_udp: i32,

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
    clock_sync_exchanges_per_interval: i32,

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
            socket_descriptor_udp: -1,
            clock_sync_global_status: ClockSyncStat::ClockSyncInit,
            clock_sync_period_ns: 10 * 1000000,
            clock_sync_exchanges_per_interval: 10,
            authentication_enabled: false,
            stop_in_progress: false,
        }
    }

    pub fn base(&mut self) -> &mut RTICommon {
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

    pub fn final_port_udp(&self) -> u16 {
        self.final_port_udp
    }

    pub fn clock_sync_global_status(&self) -> ClockSyncStat {
        self.clock_sync_global_status.clone()
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

    pub fn set_port(&mut self, user_specified_port: u16) {
        self.user_specified_port = user_specified_port;
    }

    pub fn set_stop_in_progress(&mut self, stop_in_progress: bool) {
        self.stop_in_progress = stop_in_progress;
    }
}
