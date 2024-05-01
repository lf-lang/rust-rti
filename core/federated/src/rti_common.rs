use crate::net_common::MsgType;
use crate::net_util::NetUtil;
/**
 * @file enclave.rs
 * @author Edward A. Lee (eal@berkeley.edu)
 * @author Soroush Bateni (soroush@utdallas.edu)
 * @author Erling Jellum (erling.r.jellum@ntnu.no)
 * @author Chadlia Jerad (chadlia.jerad@ensi-uma.tn)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @author Hokeun Kim (hokeun@asu.edu)
 * @copyright (c) 2020-2023, The University of California at Berkeley
 * License in [BSD 2-clause](https://github.com/lf-lang/reactor-c/blob/main/LICENSE.md)
 * @brief Common declarations for runtime infrastructure (RTI) for scheduling enclaves
 * and distributed Lingua Franca programs.
 */
use crate::rti_remote::RTIRemote;
use crate::tag;
use crate::tag::{Instant, Interval, Tag, FOREVER};
use crate::trace::{Trace, TraceDirection, TraceEvent};
use crate::FederateInfo;
use crate::SchedulingNodeState::*;

use std::io::Write;
use std::mem;
use std::sync::{Arc, Condvar, Mutex, RwLock};

const IS_IN_ZERO_DELAY_CYCLE: i32 = 1;
const IS_IN_CYCLE: i32 = 2;

/** Mode of execution of a federate. */
#[derive(PartialEq)]
enum ExecutionMode {
    FAST,
    REALTIME,
}

#[derive(PartialEq, Clone, Debug)]
pub enum SchedulingNodeState {
    NotConnected, // The scheduling node has not connected.
    Granted,      // Most recent MsgType::NextEventTag has been granted.
    Pending,      // Waiting for upstream scheduling nodes.
}

/** Struct for minimum delays from upstream nodes. */
#[derive(PartialEq, Clone)]
pub struct MinimumDelay {
    id: i32,        // ID of the upstream node.
    min_delay: Tag, // Minimum delay from upstream.
}

impl MinimumDelay {
    pub fn new(id: i32, min_delay: Tag) -> MinimumDelay {
        MinimumDelay { id, min_delay }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn min_delay(&self) -> &Tag {
        &self.min_delay
    }
}
/**
 * Information about the scheduling nodes coordinated by the RTI.
 * The abstract scheduling node could either be an enclave or a federate.
 * The information includes its runtime state,
 * mode of execution, and connectivity with other scheduling nodes.
 * The list of upstream and downstream scheduling nodes does not include
 * those that are connected via a "physical" connection (one
 * denoted with ~>) because those connections do not impose
 * any scheduling constraints.
 */
#[derive(PartialEq)]
pub struct SchedulingNode {
    id: u16,                         // ID of this scheduling node.
    completed: Tag, // The largest logical tag completed by the federate (or NEVER if no LTC has been received).
    last_granted: Tag, // The maximum Tag that has been granted so far (or NEVER if none granted)
    last_provisionally_granted: Tag, // The maximum PTAG that has been provisionally granted (or NEVER if none granted)
    next_event: Tag, // Most recent NET received from the federate (or NEVER if none received).
    state: SchedulingNodeState, // State of the federate.
    upstream: Vec<i32>, // Array of upstream federate ids.
    upstream_delay: Vec<Interval>, // Minimum delay on connections from upstream federates.
    // Here, NEVER encodes no delay. 0LL is a microstep delay.
    num_upstream: i32,    // Size of the array of upstream federates and delays.
    downstream: Vec<i32>, // Array of downstream federate ids.
    num_downstream: i32,  // Size of the array of downstream federates.
    mode: ExecutionMode,  // FAST or REALTIME.
    min_delays: Vec<MinimumDelay>, // Array of minimum delays from upstream nodes, not including this node.
    num_min_delays: u64,           // Size of min_delays array.
    flags: i32,                    // Or of IS_IN_ZERO_DELAY_CYCLE, IS_IN_CYCLE
}

impl SchedulingNode {
    pub fn new() -> SchedulingNode {
        SchedulingNode {
            id: 0,
            completed: Tag::never_tag(),
            last_granted: Tag::never_tag(),
            last_provisionally_granted: Tag::never_tag(),
            next_event: Tag::never_tag(),
            state: SchedulingNodeState::NotConnected,
            upstream: Vec::new(),
            upstream_delay: Vec::new(),
            num_upstream: 0,
            downstream: Vec::new(),
            num_downstream: 0,
            mode: ExecutionMode::REALTIME,
            min_delays: Vec::new(),
            num_min_delays: 0,
            flags: 0,
        }
    }

    pub fn initialize_scheduling_node(&mut self, id: u16) {
        self.id = id;
        // Initialize the next event condition variable.
        // TODO: lf_cond_init(&e->next_event_condition, &rti_mutex);
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn completed(&self) -> Tag {
        self.completed.clone()
    }

    pub fn last_granted(&self) -> Tag {
        self.last_granted.clone()
    }

    pub fn last_provisionally_granted(&self) -> Tag {
        self.last_provisionally_granted.clone()
    }

    pub fn next_event(&self) -> Tag {
        self.next_event.clone()
    }

    pub fn state(&self) -> SchedulingNodeState {
        self.state.clone()
    }

    pub fn upstream(&self) -> &Vec<i32> {
        &self.upstream
    }

    pub fn upstream_delay(&self) -> &Vec<Interval> {
        &self.upstream_delay
    }

    pub fn num_upstream(&self) -> i32 {
        self.num_upstream
    }

    pub fn downstream(&self) -> &Vec<i32> {
        &self.downstream
    }

    pub fn num_downstream(&self) -> i32 {
        self.num_downstream
    }

    pub fn min_delays(&self) -> &Vec<MinimumDelay> {
        &self.min_delays
    }

    pub fn min_delays_mut(&mut self) -> &mut Vec<MinimumDelay> {
        &mut self.min_delays
    }

    pub fn num_min_delays(&self) -> u64 {
        self.num_min_delays
    }

    pub fn flags(&self) -> i32 {
        self.flags
    }

    pub fn set_last_granted(&mut self, tag: Tag) {
        self.last_granted = tag;
    }

    pub fn set_last_provisionally_granted(&mut self, tag: Tag) {
        self.last_provisionally_granted = tag;
    }

    pub fn set_next_event(&mut self, next_event_tag: Tag) {
        self.next_event = next_event_tag;
    }

    pub fn set_state(&mut self, state: SchedulingNodeState) {
        self.state = state;
    }

    pub fn set_upstream_id_at(&mut self, upstream_id: u16, idx: usize) {
        // TODO: Handle the case when idx > upstream size.
        self.upstream.insert(idx, upstream_id as i32);
    }

    pub fn set_completed(&mut self, completed: Tag) {
        self.completed = completed.clone()
    }

    pub fn set_upstream_delay_at(&mut self, upstream_delay: tag::Interval, idx: usize) {
        // TODO: Handle the case when idx > upstream_delay size.
        self.upstream_delay.insert(idx, upstream_delay);
    }

    pub fn set_num_upstream(&mut self, num_upstream: i32) {
        self.num_upstream = num_upstream;
    }

    pub fn set_downstream_id_at(&mut self, downstream_id: u16, idx: usize) {
        // TODO: Handle the case when idx > downstream size.
        self.downstream.insert(idx, downstream_id as i32);
    }

    pub fn set_num_downstream(&mut self, num_downstream: i32) {
        self.num_downstream = num_downstream;
    }

    pub fn set_min_delays(&mut self, min_delays: Vec<MinimumDelay>) {
        self.min_delays = min_delays;
    }

    pub fn set_num_min_delays(&mut self, num_min_delays: u64) {
        self.num_min_delays = num_min_delays;
    }

    pub fn set_flags(&mut self, flags: i32) {
        self.flags = flags;
    }

    /**
     * @brief Update the next event tag of an scheduling node.
     *
     * This will notify downstream scheduling nodes with a TAG or PTAG if appropriate.
     *
     * This function assumes that the caller is holding the RTI mutex.
     */
    pub fn update_scheduling_node_next_event_tag_locked(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        next_event_tag: Tag,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let num_upstream;
        let number_of_scheduling_nodes;
        {
            let mut locked_rti = _f_rti.write().unwrap();
            number_of_scheduling_nodes = locked_rti.base().number_of_scheduling_nodes();
            let idx: usize = fed_id.into();
            let fed = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            let e = fed.enclave_mut();
            e.set_next_event(next_event_tag.clone());
            num_upstream = e.num_upstream();
        }
        println!(
            "RTI: Updated the recorded next event tag for federate/enclave {} to ({},{})",
            fed_id,
            next_event_tag.time() - start_time,
            next_event_tag.microstep()
        );

        // Check to see whether we can reply now with a tag advance grant.
        // If the enclave has no upstream enclaves, then it does not wait for
        // nor expect a reply. It just proceeds to advance time.
        if num_upstream > 0 {
            Self::notify_advance_grant_if_safe(
                _f_rti.clone(),
                fed_id,
                number_of_scheduling_nodes,
                start_time,
                sent_start_time.clone(),
            );
        } else {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let fed = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            let e = fed.enclave_mut();
            e.set_last_granted(next_event_tag.clone());
        }
        // Check downstream enclaves to see whether they should now be granted a TAG.
        // To handle cycles, need to create a boolean array to keep
        // track of which upstream enclaves have been visited.
        let mut visited = vec![false as bool; number_of_scheduling_nodes as usize]; // Initializes to 0.
        Self::notify_downstream_advance_grant_if_safe(
            _f_rti.clone(),
            fed_id,
            number_of_scheduling_nodes,
            start_time,
            &mut visited,
            sent_start_time,
        );
    }

    /**
     * @brief Either send to a federate or unblock an enclave to give it a tag.
     * This function requires two different implementations, one for enclaves
     * and one for federates.
     *
     * This assumes the caller holds the RTI mutex.
     */
    fn notify_advance_grant_if_safe(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        number_of_enclaves: i32,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        let grant = Self::tag_advance_grant_if_safe(_f_rti.clone(), fed_id, start_time);
        if Tag::lf_tag_compare(&grant.tag(), &Tag::never_tag()) != 0 {
            if grant.is_provisional() {
                Self::notify_provisional_tag_advance_grant(
                    _f_rti,
                    fed_id,
                    number_of_enclaves,
                    grant.tag(),
                    start_time,
                    sent_start_time,
                );
            } else {
                Self::notify_tag_advance_grant(
                    _f_rti,
                    fed_id,
                    grant.tag(),
                    start_time,
                    sent_start_time,
                );
            }
        }
    }

    /**
     * Determine whether the specified scheduling node is eligible for a tag advance grant,
     * (TAG) and, if so, return the details. This is called upon receiving a LTC, NET
     * or resign from an upstream node.
     *
     * This function calculates the minimum M over
     * all upstream scheduling nodes of the "after" delay plus the most recently
     * received LTC from that node. If M is greater than the
     * most recent TAG to e or greater than or equal to the most
     * recent PTAG, then return TAG(M).
     *
     * If the above conditions do not result in returning a TAG, then find the
     * minimum M of the earliest possible future message from upstream federates.
     * This is calculated by transitively looking at the most recently received
     * NET calls from upstream scheduling nodes.
     * If M is greater than the NET of e or the most recent PTAG to e, then
     * return a TAG with tag equal to the NET of e or the PTAG.
     * If M is equal to the NET of the federate, then return PTAG(M).
     *
     * This should be called whenever an immediately upstream federate sends to
     * the RTI an LTC (latest tag complete), or when a transitive upstream
     * federate sends a NET (Next Event Tag) message.
     * It is also called when an upstream federate resigns from the federation.
     *
     * This function assumes that the caller holds the RTI mutex.
     */
    fn tag_advance_grant_if_safe(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        start_time: Instant,
    ) -> TagAdvanceGrant {
        let mut result = TagAdvanceGrant::new(Tag::never_tag(), false);

        // Find the earliest LTC of upstream scheduling_nodes (M).
        {
            let mut min_upstream_completed = Tag::forever_tag();

            let locked_rti = _f_rti.read().unwrap();
            let scheduling_nodes = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            let e = scheduling_nodes[idx].enclave();
            let upstreams = e.upstream();
            let upstream_delay = e.upstream_delay();
            for j in 0..upstreams.len() {
                let delay = upstream_delay[j];
                let upstream = &scheduling_nodes[upstreams[j] as usize].enclave();
                // Ignore this enclave if it no longer connected.
                if upstream.state() == SchedulingNodeState::NotConnected {
                    continue;
                }

                // Adjust by the "after" delay.
                // Note that "no delay" is encoded as NEVER,
                // whereas one microstep delay is encoded as 0LL.
                let candidate = Tag::lf_delay_strict(&upstream.completed(), delay);

                if Tag::lf_tag_compare(&candidate, &min_upstream_completed) < 0 {
                    min_upstream_completed = candidate.clone();
                }
            }
            if min_upstream_completed.time() >= start_time {
                println!(
                    "Minimum upstream LTC for federate/enclave {} is ({},{}) (adjusted by after delay).",
                    e.id(),
                    min_upstream_completed.time() - start_time,
                    min_upstream_completed.microstep()
                );
            } else {
                println!(
                    "[tag_advance_grant_if_safe:396, federate/enclave {}]   WARNING!!! min_upstream_completed.time({}) < start_time({})",
                    e.id(),
                    // FIXME: Check the below calculation
                    min_upstream_completed.time(), // - start_time,
                    min_upstream_completed.microstep()
                );
            }
            if Tag::lf_tag_compare(&min_upstream_completed, &e.last_granted()) > 0
                && Tag::lf_tag_compare(&min_upstream_completed, &e.next_event()) >= 0
            // The enclave has to advance its tag
            {
                result.set_tag(min_upstream_completed);
                return result;
            }
        }

        // Can't make progress based only on upstream LTCs.
        // If all (transitive) upstream enclaves of the enclave
        // have earliest event tags such that the
        // enclave can now advance its tag, then send it a TAG message.
        // Find the tag of the earliest event that may be later received from an upstream enclave
        // or federate (which includes any after delays on the connections).
        let t_d =
            Self::earliest_future_incoming_message_tag(_f_rti.clone(), fed_id as u16, start_time);
        // Non-ZDC version of the above. This is a tag that must be strictly greater than
        // that of the next granted PTAG.
        let t_d_strict = Self::eimt_strict(_f_rti.clone(), fed_id as u16, start_time);

        if t_d.time() >= start_time {
            println!(
                "RTI: Earliest next event upstream of node {} has tag ({},{}).",
                fed_id,
                t_d.time() - start_time,
                t_d.microstep()
            );
        } else {
            println!(
                "[tag_advance_grant_if_safe:432]   WARNING!!! t_d.time < start_time   ({},{}",
                // FIXME: Check the below calculation
                t_d.time(), // - start_time,
                start_time
            );
        }

        // Given an EIMT (earliest incoming message tag) there are these possible scenarios:
        //  1) The EIMT is greater than the NET we want to advance to. Grant a TAG.
        //  2) The EIMT is equal to the NET and the strict EIMT is greater than the net
        //     and the federate is part of a zero-delay cycle (ZDC).  Grant a PTAG.
        //  3) Otherwise, grant nothing and wait for further updates.
        let next_event;
        let last_provisionally_granted;
        let last_granted;
        {
            let locked_rti = _f_rti.read().unwrap();
            let scheduling_nodes = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            let e = scheduling_nodes[idx].enclave();
            next_event = e.next_event();
            last_provisionally_granted = e.last_provisionally_granted();
            last_granted = e.last_granted();
        }

        // Scenario (1) above
        if Tag::lf_tag_compare(&t_d, &next_event) > 0                      // EIMT greater than NET
            && Tag::lf_tag_compare(&next_event, &Tag::never_tag()) > 0      // NET is not NEVER_TAG
            && Tag::lf_tag_compare(&t_d, &last_provisionally_granted) >= 0  // The grant is not redundant
                                                                        // (equal is important to override any previous
                                                                        // PTAGs).
            // The grant is not redundant.
            && Tag::lf_tag_compare(&t_d, &last_granted) > 0
        {
            // No upstream node can send events that will be received with a tag less than or equal to
            // e->next_event, so it is safe to send a TAG.
            if next_event.time() >= start_time {
                println!("RTI: Earliest upstream message time for fed/encl {} is ({},{})(adjusted by after delay). Granting tag advance (TAG) for ({},{})",
                    fed_id,
                    t_d.time() - start_time, t_d.microstep(),
                    next_event.time() - start_time,
                    next_event.microstep());
            } else {
                println!("[tag_advance_grant_if_safe:471]   WARNING!!! t_d.time({}) or next_event.time({}) < start_time({})", 
                // FIXME: Check the below calculation
                t_d.time(), // - start_time,
                next_event.time(), // - start_time,
                start_time);
            }
            result.set_tag(next_event);
        } else if
        // Scenario (2) above
        Tag::lf_tag_compare(&t_d, &next_event) == 0                         // EIMT equal to NET
            && Self::is_in_zero_delay_cycle(_f_rti.clone(), fed_id)         // The node is part of a ZDC
            && Tag::lf_tag_compare(&t_d_strict, &next_event) > 0             // The strict EIMT is greater than the NET
            && Tag::lf_tag_compare(&t_d, &last_provisionally_granted) > 0   // The grant is not redundant
            // The grant is not redundant.
            && Tag::lf_tag_compare(&t_d, &last_granted) > 0
        {
            // Some upstream node may send an event that has the same tag as this node's next event,
            // so we can only grant a PTAG.
            if t_d.time() >= start_time && next_event.time() >= start_time {
                println!("RTI: Earliest upstream message time for fed/encl {} is ({},{})(adjusted by after delay). Granting provisional tag advance (PTAG) for ({},{})",
                fed_id,
                t_d.time() - start_time, t_d.microstep(),
                next_event.time() - start_time,
                next_event.microstep());
            } else {
                println!("[tag_advance_grant_if_safe:492]   WARNING!!!   next_event.time({}) or t_d.time({}) < start_time({})",
                // FIXME: Check the below calculation
                t_d.time(), // - start_time,
                next_event.time(), // - start_time,
                start_time);
            }
            result.set_tag(next_event);
            result.set_provisional(true);
        }
        result
    }

    /**
     * Given a node (enclave or federate), find the tag of the earliest possible incoming
     * message (EIMT) from upstream enclaves or federates, which will be the smallest upstream NET
     * plus the least delay. This could be NEVER_TAG if the RTI has not seen a NET from some
     * upstream node.
     */
    fn earliest_future_incoming_message_tag(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        start_time: Instant,
    ) -> Tag {
        // First, we need to find the shortest path (minimum delay) path to each upstream node
        // and then find the minimum of the node's recorded NET plus the minimum path delay.
        // Update the shortest paths, if necessary.
        let is_first_time;
        {
            let locked_rti = _f_rti.read().unwrap();
            let scheduling_nodes = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            is_first_time = scheduling_nodes[idx].enclave().min_delays().len() == 0;
        }
        if is_first_time {
            Self::update_min_delays_upstream(_f_rti.clone(), fed_id);
        }

        // Next, find the tag of the earliest possible incoming message from upstream enclaves or
        // federates, which will be the smallest upstream NET plus the least delay.
        // This could be NEVER_TAG if the RTI has not seen a NET from some upstream node.
        let mut t_d = Tag::forever_tag();
        let num_min_delays;
        {
            let locked_rti = _f_rti.read().unwrap();
            let enclaves = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            let fed: &FederateInfo = &enclaves[idx];
            let e = fed.enclave();
            num_min_delays = e.num_min_delays();
        }
        for i in 0..num_min_delays {
            let upstream_id;
            {
                let locked_rti = _f_rti.read().unwrap();
                let enclaves = locked_rti.base().scheduling_nodes();
                let idx: usize = fed_id.into();
                let fed: &FederateInfo = &enclaves[idx];
                let e = fed.enclave();
                upstream_id = e.min_delays[i as usize].id() as usize;
            }
            let upstream_next_event;
            {
                // Node e->min_delays[i].id is upstream of e with min delay e->min_delays[i].min_delay.
                let mut locked_rti = _f_rti.write().unwrap();
                let fed: &mut FederateInfo =
                    &mut locked_rti.base_mut().scheduling_nodes_mut()[upstream_id];
                let upstream = fed.enclave_mut();
                // If we haven't heard from the upstream node, then assume it can send an event at the start time.
                upstream_next_event = upstream.next_event();
                if Tag::lf_tag_compare(&upstream_next_event, &Tag::never_tag()) == 0 {
                    let start_tag = Tag::new(start_time, 0);
                    upstream.set_next_event(start_tag);
                }
            }
            // The min_delay here is a tag_t, not an interval_t because it may account for more than
            // one connection. No delay at all is represented by (0,0). A delay of 0 is represented
            // by (0,1). If the time part of the delay is greater than 0, then we want to ignore
            // the microstep in upstream.next_event() because that microstep will have been lost.
            // Otherwise, we want preserve it and add to it. This is handled by lf_tag_add().
            let min_delay;
            let earliest_tag_from_upstream;
            {
                let locked_rti = _f_rti.read().unwrap();
                let idx: usize = fed_id.into();
                let fed = &locked_rti.base().scheduling_nodes()[idx];
                let e = fed.enclave();
                min_delay = e.min_delays()[i as usize].min_delay();
                earliest_tag_from_upstream = Tag::lf_tag_add(&upstream_next_event, &min_delay);
                /* Following debug message is too verbose for normal use:
                if earliest_tag_from_upstream.time() >= start_time {
                    println!("RTI: Earliest next event upstream of fed/encl {} at fed/encl {} has tag ({},{}).",
                        fed_id,
                        upstream_id,
                        earliest_tag_from_upstream.time() - start_time,
                        earliest_tag_from_upstream.microstep());
                } else {
                    println!(
                        "    earliest_tag_from_upstream.time() < start_time,   ({},{})",
                        earliest_tag_from_upstream.time(),
                        start_time
                    );
                }
                */
            }
            if Tag::lf_tag_compare(&earliest_tag_from_upstream, &t_d) < 0 {
                t_d = earliest_tag_from_upstream.clone();
            }
        }
        t_d
    }

    /**
     * For the given scheduling node (enclave or federate), if necessary, update the `min_delays`,
     * `num_min_delays`, and the fields that indicate cycles.  These fields will be
     * updated only if they have not been previously updated or if invalidate_min_delays_upstream
     * has been called since they were last updated.
     */
    fn update_min_delays_upstream(_f_rti: Arc<RwLock<RTIRemote>>, node_idx: u16) {
        let num_min_delays;
        let number_of_scheduling_nodes;
        {
            let locked_rti = _f_rti.read().unwrap();
            let scheduling_nodes = locked_rti.base().scheduling_nodes();
            let idx: usize = node_idx.into();
            num_min_delays = scheduling_nodes[idx].enclave().min_delays().len();
            number_of_scheduling_nodes = locked_rti.base().number_of_scheduling_nodes();
        }
        // Check whether cached result is valid.
        if num_min_delays == 0 {
            // This is not Dijkstra's algorithm, but rather one optimized for sparse upstream nodes.
            // There must be a name for this algorithm.

            // Array of results on the stack:
            let mut path_delays = Vec::new();
            // This will be the number of non-FOREVER entries put into path_delays.
            let mut count: u64 = 0;

            for _i in 0..number_of_scheduling_nodes {
                path_delays.push(Tag::forever_tag());
            }
            // FIXME:: Handle "as i32" properly.
            Self::_update_min_delays_upstream(
                _f_rti.clone(),
                node_idx as i32,
                -1,
                &mut path_delays,
                &mut count,
            );

            // Put the results onto the node's struct.
            {
                let mut locked_rti = _f_rti.write().unwrap();
                let scheduling_nodes = locked_rti.base_mut().scheduling_nodes_mut();
                let idx: usize = node_idx.into();
                let node = scheduling_nodes[idx].enclave_mut();
                node.set_num_min_delays(count);
                node.set_min_delays(Vec::new());
                println!(
                    "++++ Node {}(is in ZDC: {}),  COUNT = {},  flags = {},  number_of_scheduling_nodes = {}\n",
                    node_idx,
                    node.flags() & IS_IN_ZERO_DELAY_CYCLE, count, node.flags(), number_of_scheduling_nodes
                );
                let mut k = 0;
                for i in 0..number_of_scheduling_nodes {
                    if Tag::lf_tag_compare(&path_delays[i as usize], &Tag::forever_tag()) < 0 {
                        // Node i is upstream.
                        if k >= count {
                            println!(
                                "Internal error! Count of upstream nodes {} for node {} is wrong!",
                                count, i
                            );
                            std::process::exit(1);
                        }
                        let min_delay = MinimumDelay::new(i, path_delays[i as usize].clone());
                        if node.min_delays().len() > k as usize {
                            let _ = std::mem::replace(
                                &mut node.min_delays_mut()[k as usize],
                                min_delay,
                            );
                        } else {
                            node.min_delays_mut().insert(k as usize, min_delay);
                        }
                        k = k + 1;
                        // N^2 debug statement could be a problem with large benchmarks.
                        // println!(
                        //     "++++ Node {} is upstream with delay ({},{}),  k = {}",
                        //     i,
                        //     path_delays[i as usize].time(),
                        //     path_delays[i as usize].microstep(),
                        //     k
                        // );
                    }
                }
            }
        }
    }

    fn is_in_zero_delay_cycle(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16) -> bool {
        let is_first_time;
        {
            let locked_rti = _f_rti.read().unwrap();
            let scheduling_nodes = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            is_first_time = scheduling_nodes[idx].enclave().min_delays().len() == 0;
        }
        if is_first_time {
            Self::update_min_delays_upstream(_f_rti.clone(), fed_id);
        }
        let flags;
        {
            let locked_rti = _f_rti.read().unwrap();
            let scheduling_nodes = locked_rti.base().scheduling_nodes();
            let idx: usize = fed_id.into();
            let node = scheduling_nodes[idx].enclave();
            flags = node.flags()
        }
        (flags & IS_IN_ZERO_DELAY_CYCLE) != 0
    }

    /**
     * Given a node (enclave or federate), find the earliest incoming message tag (EIMT) from
     * any immediately upstream node that is not part of zero-delay cycle (ZDC).
     * These tags are treated strictly by the RTI when deciding whether to grant a PTAG.
     * Since the upstream node is not part of a ZDC, there is no need to block on the input
     * from that node since we can simply wait for it to complete its tag without chance of
     * introducing a deadlock.  This will return FOREVER_TAG if there are no non-ZDC upstream nodes.
     * @return The earliest possible incoming message tag from a non-ZDC upstream node.
     */
    fn eimt_strict(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16, start_time: Instant) -> Tag {
        // Find the tag of the earliest possible incoming message from immediately upstream
        // enclaves or federates that are not part of a zero-delay cycle.
        // This will be the smallest upstream NET plus the least delay.
        // This could be NEVER_TAG if the RTI has not seen a NET from some upstream node.
        let num_upstream;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let e = locked_rti.base().scheduling_nodes()[idx].enclave();
            num_upstream = e.num_upstream();
        }
        let mut t_d = Tag::forever_tag();
        for i in 0..num_upstream {
            let upstream_id;
            let upstream_delay;
            let next_event;
            {
                let locked_rti = _f_rti.read().unwrap();
                let scheduling_nodes = locked_rti.base().scheduling_nodes();
                let idx: usize = fed_id.into();
                let e = scheduling_nodes[idx].enclave();
                // let upstreams = e.upstream();
                // let upstream_id = upstreams[i] as usize;
                upstream_id = e.upstream()[i as usize] as usize;
                upstream_delay = e.upstream_delay()[i as usize];
                next_event = e.next_event();
            }
            // Skip this node if it is part of a zero-delay cycle.
            if Self::is_in_zero_delay_cycle(_f_rti.clone(), upstream_id as u16) {
                continue;
            }
            // If we haven't heard from the upstream node, then assume it can send an event at the start time.
            if Tag::lf_tag_compare(&next_event, &Tag::never_tag()) == 0 {
                let mut locked_rti = _f_rti.write().unwrap();
                let scheduling_nodes = locked_rti.base_mut().scheduling_nodes_mut();
                let upstream = scheduling_nodes[upstream_id].enclave_mut();
                let start_tag = Tag::new(start_time, 0);
                upstream.set_next_event(start_tag);
            }
            // Need to consider nodes that are upstream of the upstream node because those
            // nodes may send messages to the upstream node.
            let mut earliest = Self::earliest_future_incoming_message_tag(
                _f_rti.clone(),
                upstream_id as u16,
                start_time,
            );
            // If the next event of the upstream node is earlier, then use that.
            if Tag::lf_tag_compare(&next_event, &earliest) < 0 {
                earliest = next_event;
            }
            let earliest_tag_from_upstream = Tag::lf_delay_tag(&earliest, upstream_delay);
            if earliest_tag_from_upstream.time() >= start_time {
                println!(
                    "RTI: Strict EIMT of fed/encl {} at fed/encl {} has tag ({},{}).",
                    fed_id,
                    upstream_id,
                    earliest_tag_from_upstream.time() - start_time,
                    earliest_tag_from_upstream.microstep()
                );
            } else {
                println!(
                    "[eimt_strict:782]   WARNING!!!   RTI: Strict EIMT of fed/encl {} at fed/encl {} -> earliest_tag_from_upstream.time() < start_timehas tag = ({} < {}).",
                    fed_id,
                    upstream_id,
                    // FIXME: Check the below calculation
                    earliest_tag_from_upstream.time(), // - start_time,
                    earliest_tag_from_upstream.microstep()
                );
            }
            if Tag::lf_tag_compare(&earliest_tag_from_upstream, &t_d) < 0 {
                t_d = earliest_tag_from_upstream;
            }
        }
        t_d
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
    fn notify_tag_advance_grant(
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
    fn notify_provisional_tag_advance_grant(
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
            let earlist =
                Self::earliest_future_incoming_message_tag(_f_rti.clone(), e_id as u16, start_time);

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

    // Local function used recursively to find minimum delays upstream.
    // Return in count the number of non-FOREVER_TAG entries in path_delays[].
    fn _update_min_delays_upstream(
        _f_rti: Arc<RwLock<RTIRemote>>,
        end_idx: i32,
        mut intermediate_idx: i32,
        path_delays: &mut Vec<Tag>,
        count: &mut u64,
    ) {
        // On first call, intermediate will be NULL, so the path delay is initialized to zero.
        let mut delay_from_intermediate_so_far = Tag::zero_tag();
        if intermediate_idx < 0 {
            intermediate_idx = end_idx;
        } else {
            // Not the first call, so intermediate is upstream of end.
            delay_from_intermediate_so_far = path_delays[intermediate_idx as usize].clone();
        }
        {
            let locked_rti = _f_rti.read().unwrap();
            let fed: &FederateInfo =
                &locked_rti.base().scheduling_nodes()[intermediate_idx as usize];
            let intermediate = fed.enclave();
            if intermediate.state() == SchedulingNodeState::NotConnected {
                // Enclave or federate is not connected.
                // No point in checking upstream scheduling_nodes.
                return;
            }
        }
        // Check nodes upstream of intermediate (or end on first call).
        // NOTE: It would be better to iterate through these sorted by minimum delay,
        // but for most programs, the gain might be negligible since there are relatively few
        // upstream nodes.
        let num_upstream;
        {
            let locked_rti = _f_rti.read().unwrap();
            let fed: &FederateInfo =
                &locked_rti.base().scheduling_nodes()[intermediate_idx as usize];
            let e = fed.enclave();
            num_upstream = e.num_upstream();
        }
        for i in 0..num_upstream {
            let upstream_id;
            let upstream_delay;
            {
                let locked_rti = _f_rti.read().unwrap();
                let scheduling_nodes = locked_rti.base().scheduling_nodes();
                let e = scheduling_nodes[intermediate_idx as usize].enclave();
                upstream_id = e.upstream[i as usize];
                upstream_delay = e.upstream_delay[i as usize];
            }
            // Add connection delay to path delay so far.
            let path_delay = Tag::lf_delay_tag(&delay_from_intermediate_so_far, upstream_delay);
            // If the path delay is less than the so-far recorded path delay from upstream, update upstream.
            if Tag::lf_tag_compare(&path_delay, &path_delays[upstream_id as usize]) < 0 {
                if path_delays[upstream_id as usize].time() == FOREVER {
                    // Found a finite path.
                    *count = *count + 1;
                }
                if path_delays.len() > upstream_id as usize {
                    let _ = std::mem::replace(
                        &mut path_delays[upstream_id as usize],
                        path_delay.clone(),
                    );
                } else {
                    path_delays.insert(upstream_id as usize, path_delay.clone());
                }
                // Since the path delay to upstream has changed, recursively update those upstream of it.
                // Do not do this, however, if the upstream node is the end node because this means we have
                // completed a cycle.
                if end_idx != upstream_id {
                    Self::_update_min_delays_upstream(
                        _f_rti.clone(),
                        end_idx,
                        upstream_id,
                        path_delays,
                        count,
                    );
                } else {
                    let mut locked_rti = _f_rti.write().unwrap();
                    let end: &mut SchedulingNode = locked_rti.base_mut().scheduling_nodes_mut()
                        [end_idx as usize]
                        .enclave_mut();
                    // Found a cycle.
                    end.set_flags(end.flags() | IS_IN_CYCLE);
                    // Is it a zero-delay cycle?
                    if Tag::lf_tag_compare(&path_delay, &Tag::zero_tag()) == 0
                        && upstream_delay < Some(0)
                    {
                        end.set_flags(end.flags() | IS_IN_ZERO_DELAY_CYCLE);
                    } else {
                        // Clear the flag.
                        end.set_flags(end.flags() & !IS_IN_ZERO_DELAY_CYCLE);
                    }
                }
            }
        }
    }

    /**
     * For all scheduling nodes downstream of the specified node, determine
     * whether they should be notified of a TAG or PTAG and notify them if so.
     *
     * This assumes the caller holds the RTI mutex.
     */
    pub fn notify_downstream_advance_grant_if_safe(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        number_of_enclaves: i32,
        start_time: Instant,
        visited: &mut Vec<bool>,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
    ) {
        visited[fed_id as usize] = true;
        let num_downstream;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed = &locked_rti.base().scheduling_nodes()[idx];
            let e = fed.enclave();
            num_downstream = e.num_downstream();
        }
        for i in 0..num_downstream {
            let e_id;
            {
                let locked_rti = _f_rti.read().unwrap();
                let idx: usize = fed_id.into();
                let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[idx];
                let downstreams = fed.enclave().downstream();
                // FIXME: Replace "as u16" properly.
                e_id = downstreams[i as usize] as u16;
                if visited[e_id as usize] {
                    continue;
                }
            }
            Self::notify_advance_grant_if_safe(
                _f_rti.clone(),
                e_id,
                number_of_enclaves,
                start_time,
                sent_start_time.clone(),
            );
            Self::notify_downstream_advance_grant_if_safe(
                _f_rti.clone(),
                e_id,
                number_of_enclaves,
                start_time,
                visited,
                sent_start_time.clone(),
            );
        }
    }

    pub fn _logical_tag_complete(
        _f_rti: Arc<RwLock<RTIRemote>>,
        fed_id: u16,
        number_of_enclaves: i32,
        start_time: Instant,
        sent_start_time: Arc<(Mutex<bool>, Condvar)>,
        completed: Tag,
    ) {
        // FIXME: Consolidate this message with NET to get NMR (Next Message Request).
        // Careful with handling startup and shutdown.
        {
            let mut locked_rti = _f_rti.write().unwrap();
            let idx: usize = fed_id.into();
            let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
            let enclave = fed.enclave_mut();
            enclave.set_completed(completed);

            println!(
                "RTI received from federate/enclave {} the Logical Tag Complete (LTC) ({},{}).",
                enclave.id(),
                enclave.completed().time() - start_time,
                enclave.completed().microstep()
            );
        }

        // Check downstream enclaves to see whether they should now be granted a TAG.
        let num_downstream;
        {
            let locked_rti = _f_rti.read().unwrap();
            let idx: usize = fed_id.into();
            let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[idx];
            let e = fed.enclave();
            num_downstream = e.num_downstream();
        }
        for i in 0..num_downstream {
            let e_id;
            {
                let locked_rti = _f_rti.read().unwrap();
                let idx: usize = fed_id.into();
                let fed: &FederateInfo = &locked_rti.base().scheduling_nodes()[idx];
                let downstreams = fed.enclave().downstream();
                // FIXME: Replace "as u16" properly.
                e_id = downstreams[i as usize] as u16;
            }
            // Notify downstream enclave if appropriate.
            Self::notify_advance_grant_if_safe(
                _f_rti.clone(),
                e_id,
                number_of_enclaves,
                start_time,
                sent_start_time.clone(),
            );
            let mut visited = vec![false as bool; number_of_enclaves as usize]; // Initializes to 0.
                                                                                // Notify enclaves downstream of downstream if appropriate.
            Self::notify_downstream_advance_grant_if_safe(
                _f_rti.clone(),
                e_id,
                number_of_enclaves,
                start_time,
                &mut visited,
                sent_start_time.clone(),
            );
        }
    }
}

pub struct RTICommon {
    // The scheduling nodes.
    scheduling_nodes: Vec<FederateInfo>,

    // Number of scheduling nodes
    number_of_scheduling_nodes: i32,

    // RTI's decided stop tag for the scheduling nodes
    max_stop_tag: Tag,

    // Number of scheduling nodes handling stop
    num_scheduling_nodes_handling_stop: i32,

    // Boolean indicating that tracing is enabled.
    tracing_enabled: bool,

    // Pointer to a tracing object
    trace: Trace,
    // The RTI mutex for making thread-safe access to the shared state.
    // TODO: lf_mutex_t* mutex;
}

impl RTICommon {
    pub fn new() -> RTICommon {
        RTICommon {
            scheduling_nodes: Vec::new(),
            number_of_scheduling_nodes: 0,
            max_stop_tag: Tag::never_tag(),
            num_scheduling_nodes_handling_stop: 0,
            tracing_enabled: false,
            trace: Trace::trace_new(""),
        }
    }

    pub fn scheduling_nodes(&self) -> &Vec<FederateInfo> {
        &self.scheduling_nodes
    }

    pub fn scheduling_nodes_mut(&mut self) -> &mut Vec<FederateInfo> {
        &mut self.scheduling_nodes
    }

    pub fn number_of_scheduling_nodes(&self) -> i32 {
        self.number_of_scheduling_nodes
    }

    pub fn max_stop_tag(&self) -> Tag {
        self.max_stop_tag.clone()
    }

    pub fn num_scheduling_nodes_handling_stop(&self) -> i32 {
        self.num_scheduling_nodes_handling_stop
    }

    pub fn tracing_enabled(&self) -> bool {
        self.tracing_enabled
    }

    pub fn trace(&mut self) -> &mut Trace {
        &mut self.trace
    }

    pub fn set_max_stop_tag(&mut self, max_stop_tag: Tag) {
        self.max_stop_tag = max_stop_tag.clone();
    }

    pub fn set_number_of_scheduling_nodes(&mut self, number_of_scheduling_nodes: i32) {
        self.number_of_scheduling_nodes = number_of_scheduling_nodes;
    }

    pub fn set_num_scheduling_nodes_handling_stop(
        &mut self,
        num_scheduling_nodes_handling_stop: i32,
    ) {
        self.num_scheduling_nodes_handling_stop = num_scheduling_nodes_handling_stop;
    }

    pub fn set_tracing_enabled(&mut self, tracing_enabled: bool) {
        self.tracing_enabled = tracing_enabled;
    }

    pub fn set_trace(&mut self, trace: Trace) {
        self.trace = trace;
    }
}

struct TagAdvanceGrant {
    tag: Tag,             // NEVER if there is no tag advance grant.
    is_provisional: bool, // True for PTAG, false for TAG.
}

impl TagAdvanceGrant {
    pub fn new(tag: Tag, is_provisional: bool) -> TagAdvanceGrant {
        TagAdvanceGrant {
            tag,
            is_provisional,
        }
    }

    pub fn tag(&self) -> Tag {
        self.tag.clone()
    }

    pub fn is_provisional(&self) -> bool {
        self.is_provisional
    }

    pub fn set_tag(&mut self, tag: Tag) {
        self.tag = tag.clone();
    }

    pub fn set_provisional(&mut self, is_provisional: bool) {
        self.is_provisional = is_provisional;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::initialize_federates;
    use crate::initialize_rti;
    use crate::process_args;

    use rand::Rng;

    const MAX_STREAM_SIZE: usize = 10000;
    const RUST_RTI_PROGRAM_PATH: &str = "target/debug/rti";
    const RUST_RTI_NUMBER_OF_FEDERATES_OPTION: &str = "-n";
    const NUMBER_OF_FEDEATES: i32 = 2;

    #[test]
    // TODO: Better tp seperate each assert into a unit test, respectively.
    fn test_minimum_delay_positive() {
        let mut rng = rand::thread_rng();
        let id: i32 = rng.gen_range(0..i32::MAX);
        let time: i64 = rng.gen_range(0..i64::MAX);
        let microstep: u32 = rng.gen_range(0..u32::MAX);
        let min_delay = Tag::new(time, microstep);
        let minimum_delay = MinimumDelay::new(id, min_delay.clone());
        assert!(minimum_delay.id() == id);
        assert!(minimum_delay.min_delay() == &min_delay);
    }

    #[test]
    // TODO: Better tp seperate each assert into a unit test, respectively.
    fn test_scheduling_node_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let id: u16 = rng.gen_range(0..u16::MAX);
        scheduling_node.initialize_scheduling_node(id);
        assert!(scheduling_node.id() == id);
        assert!(scheduling_node.completed() == Tag::never_tag());
        assert!(scheduling_node.last_granted() == Tag::never_tag());
        assert!(scheduling_node.last_provisionally_granted() == Tag::never_tag());
        assert!(scheduling_node.next_event() == Tag::never_tag());
        assert!(scheduling_node.state() == SchedulingNodeState::NotConnected);
        assert!(scheduling_node.upstream() == &(Vec::<i32>::new()));
        assert!(scheduling_node.upstream_delay() == &(Vec::<Interval>::new()));
        assert!(scheduling_node.num_upstream() == 0);
        assert!(scheduling_node.downstream() == &(Vec::<i32>::new()));
        assert!(scheduling_node.num_downstream() == 0);
        assert!(scheduling_node.min_delays() == &(Vec::<MinimumDelay>::new()));
        assert!(scheduling_node.num_min_delays() == 0);
        assert!(scheduling_node.flags() == 0);
    }

    #[test]
    fn test_set_last_granted_positive() {
        let mut scheduling_node = SchedulingNode::new();
        scheduling_node.set_last_granted(Tag::forever_tag());
        assert!(scheduling_node.last_granted() == Tag::forever_tag());
    }

    #[test]
    fn test_set_last_provisionally_granted_positive() {
        let mut scheduling_node = SchedulingNode::new();
        scheduling_node.set_last_provisionally_granted(Tag::forever_tag());
        assert!(scheduling_node.last_provisionally_granted() == Tag::forever_tag());
    }

    #[test]
    fn test_set_next_event_positive() {
        let mut scheduling_node = SchedulingNode::new();
        scheduling_node.set_next_event(Tag::forever_tag());
        assert!(scheduling_node.next_event() == Tag::forever_tag());
    }

    #[test]
    fn test_set_state_positive() {
        let mut scheduling_node = SchedulingNode::new();
        scheduling_node.set_state(SchedulingNodeState::Granted);
        assert!(scheduling_node.state() == SchedulingNodeState::Granted);
        scheduling_node.set_state(SchedulingNodeState::Pending);
        assert!(scheduling_node.state() == SchedulingNodeState::Pending);
        scheduling_node.set_state(SchedulingNodeState::NotConnected);
        assert!(scheduling_node.state() == SchedulingNodeState::NotConnected);
    }

    #[test]
    fn test_set_upstream_id_at_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let upstream_id: u16 = rng.gen_range(1..u16::MAX);
        let idx: usize = rng.gen_range(0..MAX_STREAM_SIZE);
        for i in 0..MAX_STREAM_SIZE {
            scheduling_node.set_upstream_id_at(0, i);
        }
        scheduling_node.set_upstream_id_at(upstream_id, idx);
        assert!(scheduling_node.upstream()[idx] == upstream_id.into());
    }

    #[test]
    fn test_set_completed_positive() {
        let mut scheduling_node = SchedulingNode::new();
        scheduling_node.set_completed(Tag::forever_tag());
        assert!(scheduling_node.completed() == Tag::forever_tag());
    }

    #[test]
    fn test_set_upstream_delay_at_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let upstream_delay = rng.gen_range(1..i64::MAX);
        let idx: usize = rng.gen_range(0..MAX_STREAM_SIZE);
        for i in 0..MAX_STREAM_SIZE {
            scheduling_node.set_upstream_delay_at(Some(0), i);
        }
        scheduling_node.set_upstream_delay_at(Some(upstream_delay), idx);
        assert!(scheduling_node.upstream_delay()[idx] == Some(upstream_delay));
    }

    #[test]
    fn test_set_num_upstream_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let num_upstream: i32 = rng.gen_range(0..i32::MAX);
        scheduling_node.set_num_upstream(num_upstream);
        assert!(scheduling_node.num_upstream() == num_upstream);
    }

    #[test]
    fn test_set_downstream_id_at_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let downstream_id: u16 = rng.gen_range(1..u16::MAX);
        let idx: usize = rng.gen_range(0..MAX_STREAM_SIZE);
        for i in 0..MAX_STREAM_SIZE {
            scheduling_node.set_downstream_id_at(0, i);
        }
        scheduling_node.set_downstream_id_at(downstream_id, idx);
        assert!(scheduling_node.downstream()[idx] == downstream_id.into());
    }

    #[test]
    fn test_set_num_downstream_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let num_downstream: i32 = rng.gen_range(0..i32::MAX);
        scheduling_node.set_num_downstream(num_downstream);
        assert!(scheduling_node.num_downstream() == num_downstream);
    }

    #[test]
    fn test_set_min_delays_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let minimum_delay_id: i32 = rng.gen_range(0..i32::MAX);
        let minimum_delay = MinimumDelay::new(minimum_delay_id, Tag::forever_tag());
        let mut min_delays = Vec::new();
        min_delays.push(minimum_delay);
        scheduling_node.set_min_delays(min_delays.clone());
        assert!(scheduling_node.min_delays() == &mut min_delays);
    }

    #[test]
    fn test_set_num_min_delays_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let num_min_delays: u64 = rng.gen_range(0..u64::MAX);
        scheduling_node.set_num_min_delays(num_min_delays);
        assert!(scheduling_node.num_min_delays() == num_min_delays);
    }

    #[test]
    fn test_set_flags_positive() {
        let mut scheduling_node = SchedulingNode::new();
        let mut rng = rand::thread_rng();
        let flags: i32 = rng.gen_range(0..i32::MAX);
        scheduling_node.set_flags(flags);
        assert!(scheduling_node.flags() == flags);
    }

    #[test]
    fn test_update_scheduling_node_next_event_tag_locked_positive() {
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
        SchedulingNode::update_scheduling_node_next_event_tag_locked(
            cloned_rti,
            0,
            Tag::new(0, 0),
            0,
            cloned_sent_start_time,
        );
    }

    #[test]
    fn test_notify_advance_grant_if_safe_positive() {
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
        SchedulingNode::notify_advance_grant_if_safe(cloned_rti, 0, 2, 0, cloned_sent_start_time);
    }

    #[test]
    fn test_tag_advance_grant_if_safe_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(RUST_RTI_PROGRAM_PATH.to_string());
        args.push(RUST_RTI_NUMBER_OF_FEDERATES_OPTION.to_string());
        args.push(NUMBER_OF_FEDEATES.to_string());
        let _ = process_args(&mut rti, &args);
        initialize_federates(&mut rti);
        let arc_rti = Arc::new(RwLock::new(rti));
        let cloned_rti = Arc::clone(&arc_rti);
        SchedulingNode::tag_advance_grant_if_safe(cloned_rti, 0, 0);
    }

    #[test]
    fn test_is_in_zero_delay_cycle_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(RUST_RTI_PROGRAM_PATH.to_string());
        args.push(RUST_RTI_NUMBER_OF_FEDERATES_OPTION.to_string());
        args.push(NUMBER_OF_FEDEATES.to_string());
        let _ = process_args(&mut rti, &args);
        initialize_federates(&mut rti);
        let arc_rti = Arc::new(RwLock::new(rti));
        let cloned_rti = Arc::clone(&arc_rti);
        let result = SchedulingNode::is_in_zero_delay_cycle(cloned_rti, 0);
        assert!(result == false);
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
        SchedulingNode::notify_tag_advance_grant(
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
        SchedulingNode::notify_provisional_tag_advance_grant(
            cloned_rti,
            0,
            NUMBER_OF_FEDEATES,
            Tag::new(0, 0),
            0,
            cloned_sent_start_time,
        );
    }

    #[test]
    fn test_earliest_future_incoming_message_tag_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(RUST_RTI_PROGRAM_PATH.to_string());
        args.push(RUST_RTI_NUMBER_OF_FEDERATES_OPTION.to_string());
        args.push(NUMBER_OF_FEDEATES.to_string());
        let _ = process_args(&mut rti, &args);
        initialize_federates(&mut rti);
        let arc_rti = Arc::new(RwLock::new(rti));
        let cloned_rti = Arc::clone(&arc_rti);
        SchedulingNode::earliest_future_incoming_message_tag(cloned_rti, 0, 0);
    }

    #[test]
    fn test_update_min_delays_upstream_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(RUST_RTI_PROGRAM_PATH.to_string());
        args.push(RUST_RTI_NUMBER_OF_FEDERATES_OPTION.to_string());
        args.push(NUMBER_OF_FEDEATES.to_string());
        let _ = process_args(&mut rti, &args);
        initialize_federates(&mut rti);
        let arc_rti = Arc::new(RwLock::new(rti));
        let cloned_rti = Arc::clone(&arc_rti);
        SchedulingNode::update_min_delays_upstream(cloned_rti, 0);
    }

    #[test]
    fn test_logical_tag_complete_positive() {
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
        SchedulingNode::_logical_tag_complete(
            cloned_rti,
            0,
            NUMBER_OF_FEDEATES,
            0,
            cloned_sent_start_time,
            Tag::never_tag(),
        );
    }

    #[test]
    // TODO: Better tp seperate each assert into a unit test, respectively.
    fn test_rti_common_positive() {
        let rti_common = RTICommon::new();
        assert!(rti_common.scheduling_nodes().len() == 0);
        assert!(rti_common.number_of_scheduling_nodes() == 0);
        assert!(rti_common.max_stop_tag() == Tag::never_tag());
        assert!(rti_common.num_scheduling_nodes_handling_stop() == 0);
        assert!(rti_common.tracing_enabled() == false);
    }

    #[test]
    fn test_set_max_stop_tag_positive() {
        let mut rti_common = RTICommon::new();
        let mut rng = rand::thread_rng();
        let time: i64 = rng.gen_range(0..i64::MAX);
        let microstep: u32 = rng.gen_range(0..u32::MAX);
        let max_stop_tag = Tag::new(time, microstep);
        rti_common.set_max_stop_tag(max_stop_tag.clone());
        assert!(rti_common.max_stop_tag() == max_stop_tag);
    }

    #[test]
    fn test_set_number_of_scheduling_nodes_positive() {
        let mut rti_common = RTICommon::new();
        let mut rng = rand::thread_rng();
        let number_of_scheduling_nodes: i32 = rng.gen_range(0..i32::MAX);
        rti_common.set_number_of_scheduling_nodes(number_of_scheduling_nodes);
        assert!(rti_common.number_of_scheduling_nodes() == number_of_scheduling_nodes);
    }

    #[test]
    fn test_set_num_scheduling_nodes_handling_stop_positive() {
        let mut rti_common = RTICommon::new();
        let mut rng = rand::thread_rng();
        let num_scheduling_nodes_handling_stop: i32 = rng.gen_range(0..i32::MAX);
        rti_common.set_num_scheduling_nodes_handling_stop(num_scheduling_nodes_handling_stop);
        assert!(
            rti_common.num_scheduling_nodes_handling_stop() == num_scheduling_nodes_handling_stop
        );
    }

    #[test]
    // TODO: Better tp seperate each assert into a unit test, respectively.
    fn test_tag_advance_grant_positive() {
        let mut rng = rand::thread_rng();
        let time: i64 = rng.gen_range(0..i64::MAX);
        let microstep: u32 = rng.gen_range(0..u32::MAX);
        let tag = Tag::new(time, microstep);
        let tag_advance_grant = TagAdvanceGrant::new(tag.clone(), false);
        assert!(tag_advance_grant.tag() == tag);
        assert!(tag_advance_grant.is_provisional() == false);
    }

    #[test]
    fn test_set_tag_positive() {
        let mut tag_advance_grant = TagAdvanceGrant::new(Tag::never_tag(), false);
        let mut rng = rand::thread_rng();
        let time: i64 = rng.gen_range(0..i64::MAX);
        let microstep: u32 = rng.gen_range(0..u32::MAX);
        let tag = Tag::new(time, microstep);
        tag_advance_grant.set_tag(tag.clone());
        assert!(tag_advance_grant.tag() == tag);
    }

    #[test]
    fn test_set_provisional_positive() {
        let mut tag_advance_grant = TagAdvanceGrant::new(Tag::never_tag(), false);
        tag_advance_grant.set_provisional(true);
        assert!(tag_advance_grant.is_provisional() == true);
    }
}
