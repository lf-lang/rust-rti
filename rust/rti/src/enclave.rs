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

pub struct Enclave {
    id: u16, // ID of this enclave.
    // TODO: tag_t completed;        // The largest logical tag completed by the federate (or NEVER if no LTC has been received).
    // TODO: tag_t last_granted;     // The maximum TAG that has been granted so far (or NEVER if none granted)
    // TODO: tag_t last_provisionally_granted;      // The maximum PTAG that has been provisionally granted (or NEVER if none granted)
    // TODO: tag_t next_event;       // Most recent NET received from the federate (or NEVER if none received).
    // TODO: fed_state_t state;      // State of the federate.
    upstream: Vec<i32>, // Array of upstream federate ids.
    // TODO: interval_t* upstream_delay;    // Minimum delay on connections from upstream federates.
    // Here, NEVER encodes no delay. 0LL is a microstep delay.
    num_upstream: i32,    // Size of the array of upstream federates and delays.
    downstream: Vec<i32>, // Array of downstream federate ids.
    num_downstream: i32,  // Size of the array of downstream federates.
                          // TODO: execution_mode_t mode;  // FAST or REALTIME.
                          // TODO: lf_cond_t next_event_condition; // Condition variable used by enclaves to notify an enclave
                          // that it's call to next_event_tag() should unblock.
}
