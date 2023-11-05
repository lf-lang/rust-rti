/**
 * @file
 * @author Edward A. Lee (eal@berkeley.edu)
 * @author Soroush Bateni (soroush@utdallas.edu)
 * @author Erling Jellum (erling.r.jellum@ntnu.no)
 * @author Chadlia Jerad (chadlia.jerad@ensi-uma.tn)
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @copyright (c) 2020-2023, The University of California at Berkeley
 * License in [BSD 2-clause](..)
 * @brief Declarations for runtime infrastructure (RTI) for distributed Lingua Franca programs.
 * This file extends enclave.h with RTI features that are specific to federations and are not
 * used by scheduling enclaves.
 */
use crate::constants::*;
use crate::federate::*;

/**
 * Structure that an RTI instance uses to keep track of its own and its
 * corresponding federates' state.
 * It is a special case of `enclave_rti_t` (declared in enclave.h). Inheritence
 * is mimicked by having the first attributes to be the same as of enclave_rti_t,
 * except that enclaves attribute here is of type `federate_t**`, while it
 * is of type `enclave_t**` in `enclave_rti_t`.
 *     // **************** IMPORTANT!!! ********************
 *     // **   If you make any change to this struct,     **
 *     // **   you MUST also change  enclave_rti_t in     **
 *     // ** (enclave.h)! The change must exactly match.  **
 *     // **************************************************
 */
pub struct FederationRTI {
    ////////////////// Enclave specific attributes //////////////////

    // The federates.
    enclaves: Vec<Federate>,

    // Number of enclaves
    number_of_enclaves: i32,

    // RTI's decided stop tag for enclaves
    // TODO: max_stop_tag:Tag,

    // Number of enclaves handling stop
    num_enclaves_handling_stop: i32,

    // Boolean indicating that tracing is enabled.
    tracing_enabled: bool,

    // Pointer to a tracing object
    // TODO: trace:Trace,
    ////////////// Federation only specific attributes //////////////

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
     * that each federate only joins its assigned federation.
     */
    federation_id: String,

    /************* TCP server information *************/
    /** The desired port specified by the user on the command line. */
    user_specified_port: u16,

    /** The final port number that the TCP socket server ends up using. */
    final_port_TCP: u16,

    /** The TCP socket descriptor for the socket server. */
    socket_descriptor_TCP: i32,

    /************* UDP server information *************/
    /** The final port number that the UDP socket server ends up using. */
    final_port_UDP: u16,

    /** The UDP socket descriptor for the socket server. */
    socket_descriptor_UDP: i32,

    /************* Clock synchronization information *************/
    /* Thread performing PTP clock sync sessions periodically. */
    // TODO: lf_thread_t clock_thread;
    /**
     * Indicates whether clock sync is globally on for the federation. Federates
     * can still selectively disable clock synchronization if they wanted to.
     */
    // TODO: clock_sync_stat clock_sync_global_status;

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

impl FederationRTI {
    pub fn new() -> FederationRTI {
        FederationRTI {
            enclaves: Vec::new(),
            // enclave_rti related initializations
            // max_stop_tag: ,
            number_of_enclaves: 0,
            num_enclaves_handling_stop: 0,
            // federation_rti related initializations
            max_start_time: 0,
            num_feds_proposed_start: 0,
            // all_federates_exited:false,
            federation_id: String::from("Unidentified Federation"),
            user_specified_port: STARTING_PORT,
            final_port_TCP: 0,
            socket_descriptor_TCP: -1,
            final_port_UDP: u16::MAX,
            socket_descriptor_UDP: -1,
            // clock_sync_global_status: ,
            clock_sync_period_ns: 10 * 1000000,
            clock_sync_exchanges_per_interval: 10,
            authentication_enabled: false,
            tracing_enabled: false,
            stop_in_progress: false,
        }
    }

    pub fn enclaves(&mut self) -> &mut Vec<Federate> {
        &mut self.enclaves
    }

    pub fn number_of_enclaves(&mut self) -> i32 {
        self.number_of_enclaves
    }

    pub fn federation_id(&mut self) -> String {
        self.federation_id.clone()
    }

    pub fn user_specified_port(&mut self) -> u16 {
        self.user_specified_port
    }

    pub fn set_number_of_enclaves(&mut self, number_of_enclaves: i32) {
        self.number_of_enclaves = number_of_enclaves;
    }

    pub fn set_federation_id(&mut self, federation_id: String) {
        self.federation_id = federation_id;
    }

    pub fn set_port(&mut self, user_specified_port: u16) {
        self.user_specified_port = user_specified_port;
    }
}
