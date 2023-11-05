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
use crate::tag;

use std::option::Option;

enum ExecutionMode {
    FAST,
    REALTIME,
}

#[derive(PartialEq, Clone, Debug)]
pub enum FedState {
    NOT_CONNECTED, // The federate has not connected.
    GRANTED,       // Most recent MSG_TYPE_NEXT_EVENT_TAG has been granted.
    PENDING,       // Waiting for upstream federates.
}

pub struct Enclave {
    id: u16,                                      // ID of this enclave.
    completed: Option<tag::TAG>, // The largest logical tag completed by the federate (or NEVER if no LTC has been received).
    last_granted: Option<tag::TAG>, // The maximum TAG that has been granted so far (or NEVER if none granted)
    last_provisionally_granted: Option<tag::TAG>, // The maximum PTAG that has been provisionally granted (or NEVER if none granted)
    next_event: Option<tag::TAG>, // Most recent NET received from the federate (or NEVER if none received).
    state: FedState,              // State of the federate.
    upstream: Vec<i32>,           // Array of upstream federate ids.
    upstream_delay: Vec<tag::Interval>, // Minimum delay on connections from upstream federates.
    // Here, NEVER encodes no delay. 0LL is a microstep delay.
    num_upstream: i32,    // Size of the array of upstream federates and delays.
    downstream: Vec<i32>, // Array of downstream federate ids.
    num_downstream: i32,  // Size of the array of downstream federates.
    mode: ExecutionMode,  // FAST or REALTIME.
                          // TODO: lf_cond_t next_event_condition; // Condition variable used by enclaves to notify an enclave
                          // that it's call to next_event_tag() should unblock.
}

impl Enclave {
    pub fn new() -> Enclave {
        Enclave {
            id: 0,
            completed: None::<tag::TAG>,
            last_granted: None::<tag::TAG>,
            last_provisionally_granted: None::<tag::TAG>,
            next_event: None::<tag::TAG>,
            state: FedState::NOT_CONNECTED,
            upstream: Vec::new(),
            upstream_delay: Vec::new(),
            num_upstream: 0,
            downstream: Vec::new(),
            num_downstream: 0,
            mode: ExecutionMode::REALTIME,
            // TODO: lf_cond_t next_event_condition;
        }
    }

    pub fn initialize_enclave(&mut self) {
        // Initialize the next event condition variable.
        // TODO: lf_cond_init(&e->next_event_condition, &rti_mutex);
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn state(&self) -> FedState {
        self.state.clone()
    }

    pub fn num_upstream(&self) -> i32 {
        self.num_upstream
    }

    pub fn num_downstream(&self) -> i32 {
        self.num_downstream
    }

    pub fn set_state(&mut self, state: FedState) {
        self.state = state;
    }

    pub fn set_upstream_id_at(&mut self, upstream_id: u16, idx: usize) {
        // FIXME: Set upstream_id exactly to the idx position
        self.upstream.push(upstream_id as i32);
    }

    pub fn set_upstream_delay_at(&mut self, upstream_delay: tag::Interval, idx: usize) {
        // FIXME: Set upstream_delay exactly to the idx position
        self.upstream_delay.push(upstream_delay);
    }

    pub fn set_num_upstream(&mut self, num_upstream: i32) {
        self.num_upstream = num_upstream;
    }

    pub fn set_downstream_id_at(&mut self, downstream_id: u16, idx: usize) {
        // FIXME: Set downstream_id exactly to the idx position
        self.downstream.push(downstream_id as i32);
    }

    pub fn set_num_downstream(&mut self, num_downstream: i32) {
        self.num_downstream = num_downstream;
    }
}
