use crate::net_common::MsgType;
use crate::net_util::NetUtil;
use crate::tag;
use crate::tag::{Instant, Interval, Tag};
use crate::FedState::*;
use crate::Federate;
/**
 * @file enclave.rs
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
use crate::FederationRTI;

use std::cell::RefCell;
use std::io::Write;
use std::mem;
use std::option::Option;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

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

struct TagAdvanceGrant {
    tag: Tag,
    is_provisional: bool,
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

pub struct Enclave {
    id: u16,                         // ID of this enclave.
    completed: Tag, // The largest logical tag completed by the federate (or NEVER if no LTC has been received).
    last_granted: Tag, // The maximum Tag that has been granted so far (or NEVER if none granted)
    last_provisionally_granted: Tag, // The maximum PTAG that has been provisionally granted (or NEVER if none granted)
    next_event: Tag, // Most recent NET received from the federate (or NEVER if none received).
    state: FedState, // State of the federate.
    upstream: Vec<i32>, // Array of upstream federate ids.
    upstream_delay: Vec<Interval>, // Minimum delay on connections from upstream federates.
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
            completed: Tag::never_tag(),
            last_granted: Tag::never_tag(),
            last_provisionally_granted: Tag::never_tag(),
            next_event: Tag::never_tag(),
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

    pub fn initialize_enclave(&mut self, id: u16) {
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

    pub fn state(&self) -> FedState {
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

    pub fn num_downstream(&self) -> i32 {
        self.num_downstream
    }

    pub fn set_last_provisionally_granted(&mut self, tag: Tag) {
        self.last_provisionally_granted = tag;
    }

    pub fn set_next_event(&mut self, next_event_tag: Tag) {
        self.next_event = next_event_tag;
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

    pub fn update_enclave_next_event_tag_locked(
        _f_rti: Arc<Mutex<FederationRTI>>,
        fed_id: u16,
        next_event_tag: Tag,
        start_time: Instant,
    ) {
        let mut id = 0;
        let mut num_upstream = 0;
        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.number_of_enclaves();
            let idx: usize = fed_id.into();
            let fed: &mut Federate = &mut locked_rti.enclaves()[idx];
            let mut e = fed.enclave();
            e.set_next_event(next_event_tag.clone());

            id = e.id();
            num_upstream = e.num_upstream();
        }
        println!(
            "RTI: Updated the recorded next event tag for federate/enclave {} to ({},{})",
            id,
            next_event_tag.time() - start_time,
            next_event_tag.microstep()
        );

        // Check to see whether we can reply now with a tag advance grant.
        // If the enclave has no upstream enclaves, then it does not wait for
        // nor expect a reply. It just proceeds to advance time.
        println!("\ne->num_upstream = {}", num_upstream);
        if num_upstream > 0 {
            Self::notify_advance_grant_if_safe(_f_rti, fed_id, start_time);
        }
        // Check downstream enclaves to see whether they should now be granted a TAG.
        // To handle cycles, need to create a boolean array to keep
        // track of which upstream enclaves have been visited.
        // let mut visited = vec![false as bool; number_of_enclaves as usize]; // Initializes to 0.
        // Self::notify_downstream_advance_grant_if_safe(e, visited);
    }

    fn notify_advance_grant_if_safe(
        _f_rti: Arc<Mutex<FederationRTI>>,
        fed_id: u16,
        start_time: Instant,
    ) {
        let grant = Self::tag_advance_grant_if_safe(_f_rti.clone(), fed_id, start_time);
        if Tag::lf_tag_compare(&grant.tag(), &Tag::never_tag()) != 0 {
            if grant.is_provisional() {
                let mut number_of_enclaves = 0;
                {
                    let mut locked_rti = _f_rti.lock().unwrap();
                    // let idx: usize = fed_id.into();
                    number_of_enclaves = locked_rti.number_of_enclaves();
                }
                // let enclaves = locked_rti.enclaves();
                // let fed: &mut Federate = &mut enclaves[idx];
                // let mut e = fed.enclave();
                Self::notify_provisional_tag_advance_grant(
                    _f_rti,
                    // enclaves,
                    // e,
                    fed_id,
                    number_of_enclaves,
                    grant.tag(),
                    start_time,
                );
            } else {
                println!("CALL [notify_tag_advance_grant]");
                // Self::notify_tag_advance_grant(e, grant.tag());
            }
        }
    }

    fn tag_advance_grant_if_safe(
        _f_rti: Arc<Mutex<FederationRTI>>,
        fed_id: u16,
        start_time: Instant,
    ) -> TagAdvanceGrant {
        let mut result = TagAdvanceGrant::new(Tag::never_tag(), false);

        // Find the earliest LTC of upstream enclaves (M).
        let mut min_upstream_completed = Tag::forever_tag();

        let mut number_of_enclaves = 0;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            number_of_enclaves = locked_rti.number_of_enclaves();
        }

        let mut locked_rti = _f_rti.lock().unwrap();
        let idx: usize = fed_id.into();
        let mut enclaves = locked_rti.enclaves();
        let fed = &enclaves[idx];
        let e = fed.e();
        let mut upstreams = e.upstream();
        let mut upstream_delay = e.upstream_delay();
        for j in upstreams {
            println!("[{}]", j);
            // FIXME:: Replace "as usize" properly.
            let delay = upstream_delay[*j as usize];
            let _fed = &enclaves[*j as usize];
            let upstream = _fed.e();
            // Ignore this enclave if it no longer connected.
            if upstream.state() == FedState::NOT_CONNECTED {
                continue;
            }

            // Adjust by the "after" delay.
            // Note that "no delay" is encoded as NEVER,
            // whereas one microstep delay is encoded as 0LL.
            let candidate = Tag::lf_delay_strict(&upstream.completed(), delay);

            if Tag::lf_tag_compare(&candidate, &min_upstream_completed) < 0 {
                println!(
                    "candidate. (time, microstep) = ({},{})",
                    candidate.time(),
                    candidate.microstep()
                );
                println!(
                    "min_upstream_completed. (time, microstep) = ({},{})",
                    min_upstream_completed.time(),
                    min_upstream_completed.microstep()
                );
                // min_upstream_completed = candidate.clone();
                println!(
                    "candidate . (time, microstep) = ({},{})",
                    candidate.time(),
                    candidate.microstep()
                );
                println!(
                    "min_upstream_completed is updated. (time, microstep) = ({},{})",
                    min_upstream_completed.time(),
                    min_upstream_completed.microstep()
                );
            }
        }
        println!(
            "Minimum upstream LTC for federate/enclave {} is ({},{}) (adjusted by after delay).",
            e.id(),
            min_upstream_completed.time() - start_time,
            min_upstream_completed.microstep()
        );
        if Tag::lf_tag_compare(&min_upstream_completed, &e.last_granted()) > 0
            && Tag::lf_tag_compare(&min_upstream_completed, &e.next_event()) >= 0
        // The enclave has to advance its tag
        {
            result.set_tag(min_upstream_completed);
            return result;
        }

        // Can't make progress based only on upstream LTCs.
        // If all (transitive) upstream enclaves of the enclave
        // have earliest event tags such that the
        // enclave can now advance its tag, then send it a TAG message.
        // Find the earliest event time of each such upstream enclave,
        // adjusted by delays on the connections.

        // To handle cycles, need to create a boolean array to keep
        // track of which upstream enclave have been visited.
        let mut visited = vec![false as bool; number_of_enclaves.try_into().unwrap()];

        // Find the tag of the earliest possible incoming message from
        // upstream enclaves.
        let mut t_d_nonzero_delay = Tag::forever_tag();
        // The tag of the earliest possible incoming message from a zero-delay connection.
        // Delayed connections are not guarded from STP violations by the MLAA; this property is
        // acceptable because delayed connections impose no deadlock risk and in some cases (startup)
        // this property is necessary to avoid deadlocks. However, it requires some special care here
        // when potentially sending a PTAG because we must not send a PTAG for a tag at which data may
        // still be received over nonzero-delay connections.
        let mut t_d_zero_delay = Tag::forever_tag();
        println!(
            "NOTE: FOREVER is displayed as ({},{}) and NEVER as ({},{})",
            i64::MAX - start_time,
            u32::MAX,
            i64::MIN - start_time,
            0
        );

        for j in upstreams {
            // FIXME:: Replace "as usize" properly.
            let delay = upstream_delay[*j as usize];
            let _fed = &enclaves[*j as usize];
            let upstream = _fed.e();

            // Ignore this enclave if it is no longer connected.
            if upstream.state() == FedState::NOT_CONNECTED {
                continue;
            }

            // Find the (transitive) next event tag upstream.
            let upstream_next_event = Self::transitive_next_event(
                enclaves,
                upstream,
                upstream.next_event(),
                &mut visited,
                start_time,
            );

            println!(
                "Earliest next event upstream of fed/encl {} at fed/encl {} has tag ({},{}).",
                e.id(),
                upstream.id(),
                upstream_next_event.time() - start_time,
                upstream_next_event.microstep()
            );

            // Adjust by the "after" delay.
            // Note that "no delay" is encoded as NEVER,
            // whereas one microstep delay is encoded as 0LL.
            // FIXME:: Replace "as usize" properly.
            let candidate =
                Tag::lf_delay_strict(&upstream_next_event, e.upstream_delay[*j as usize]);

            // FIXME:: Replace "as usize" properly.
            if e.upstream_delay[*j as usize] == Some(i64::MIN) {
                if Tag::lf_tag_compare(&candidate, &t_d_zero_delay) < 0 {
                    t_d_zero_delay = candidate;
                }
            } else {
                if Tag::lf_tag_compare(&candidate, &t_d_nonzero_delay) < 0 {
                    t_d_nonzero_delay = candidate;
                }
            }
        }

        let mut t_d = Tag::forever_tag();
        if Tag::lf_tag_compare(&t_d_zero_delay, &t_d_nonzero_delay) < 0 {
            t_d = t_d_zero_delay.clone();
        } else {
            t_d = t_d_nonzero_delay.clone();
        }
        println!(
            "Earliest next event upstream has tag ({},{}).",
            t_d.time() - start_time,
            t_d.microstep()
        );

        if Tag::lf_tag_compare(&t_d, &e.next_event()) > 0       // The enclave has something to do.
            && Tag::lf_tag_compare(&t_d, &e.last_provisionally_granted()) >= 0  // The grant is not redundant
                                                                        // (equal is important to override any previous
                                                                        // PTAGs).
            && Tag::lf_tag_compare(&t_d, &e.last_granted()) > 0
        // The grant is not redundant.
        {
            // All upstream enclaves have events with a larger tag than fed, so it is safe to send a TAG.
            println!("Earliest upstream message time for fed/encl {} is ({},{}) (adjusted by after delay). Granting tag advance for ({},{})",
                    e.id(),
                    t_d.time() - start_time, t_d.microstep(),
                    e.next_event().time() - start_time,
                    e.next_event().microstep());
            result.set_tag(e.next_event());
        } else if (
            Tag::lf_tag_compare(&t_d_zero_delay, &e.next_event()) == 0      // The enclave has something to do.
            && Tag::lf_tag_compare(&t_d_zero_delay, &t_d_nonzero_delay) < 0  // The statuses of nonzero-delay connections are known at tag t_d_zero_delay
            && Tag::lf_tag_compare(&t_d_zero_delay, &e.last_provisionally_granted()) > 0  // The grant is not redundant.
            && Tag::lf_tag_compare(&t_d_zero_delay, &e.last_granted()) > 0
            // The grant is not redundant.
        ) {
            // Some upstream enclaves has an event that has the same tag as fed's next event, so we can only provisionally
            // grant a TAG (via a PTAG).
            println!("Earliest upstream message time for fed/encl {} is ({},{}) (adjusted by after delay). Granting provisional tag advance.",
                e.id(),
                t_d_zero_delay.time() - start_time, t_d_zero_delay.microstep());
            result.set_tag(t_d_zero_delay);
            result.set_provisional(true);
        }

        result
    }

    fn transitive_next_event(
        enclaves: &Vec<Federate>,
        e: &Enclave,
        candidate: Tag,
        visited: &mut Vec<bool>,
        start_time: Instant,
    ) -> Tag {
        // FIXME:: Replace "as usize" properly.
        if visited[e.id() as usize] || e.state() == FedState::NOT_CONNECTED {
            // Enclave has stopped executing or we have visited it before.
            // No point in checking upstream enclaves.
            return candidate.clone();
        }

        // FIXME:: Replace "as usize" properly.
        visited[e.id() as usize] = true;
        let mut result = e.next_event();

        // If the candidate is less than this enclave's next_event, use the candidate.
        if Tag::lf_tag_compare(&candidate, &result) < 0 {
            result = candidate.clone();
        }

        // The result cannot be earlier than the start time.
        if result.time() < start_time {
            // Earliest next event cannot be before the start time.
            result = Tag::new(start_time, 0);
        }

        // Check upstream enclaves to see whether any of them might send
        // an event that would result in an earlier next event.
        let num_upstream = e.num_upstream();
        for i in 0..num_upstream {
            // FIXME:: Replace "as usize" properly.
            let e = enclaves[e.upstream()[i as usize] as usize].e();
            let mut upstream_result =
                Self::transitive_next_event(enclaves, e, result.clone(), visited, start_time);

            // Add the "after" delay of the connection to the result.
            // FIXME:: Replace "as usize" properly.
            upstream_result = Tag::lf_delay_tag(&upstream_result, e.upstream_delay()[i as usize]);

            // If the adjusted event time is less than the result so far, update the result.
            if Tag::lf_tag_compare(&upstream_result, &result) < 0 {
                result = upstream_result;
            }
        }
        let completed = e.completed();
        if Tag::lf_tag_compare(&result, &completed) < 0 {
            result = completed;
        }

        result
    }

    fn notify_tag_advance_grant() {}

    fn notify_provisional_tag_advance_grant(
        _f_rti: Arc<Mutex<FederationRTI>>,
        fed_id: u16,
        number_of_enclaves: i32,
        tag: Tag,
        start_time: Instant,
    ) {
        let mut stream = None;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let enclaves = locked_rti.enclaves();
            let idx: usize = fed_id.into();
            let fed: &Federate = &enclaves[idx];
            let mut e = fed.e();
            stream = fed.stream().as_ref(); //.unwrap();
            if e.state() == FedState::NOT_CONNECTED
                || Tag::lf_tag_compare(&tag, &e.last_granted()) <= 0
                || Tag::lf_tag_compare(&tag, &e.last_provisionally_granted()) <= 0
            {
                return;
            }
            // Need to make sure that the destination federate's thread has already
            // sent the starting MSG_TYPE_TIMESTAMP message.
            while e.state() == FedState::PENDING {
                // Need to wait here.
                // TODO: lf_cond_wait(&sent_start_time);
            }
        }

        let message_length = 1 + mem::size_of::<i64>() + mem::size_of::<u32>();
        // FIXME:: Replace "as usize" properly.
        let mut buffer = vec![0 as u8; message_length as usize];
        buffer[0] = MsgType::PROVISIONAL_TAG_ADVANCE_GRANT.to_byte();
        NetUtil::encode_int64(tag.time(), &mut buffer, 1);
        NetUtil::encode_int32(
            tag.microstep().try_into().unwrap(),
            &mut buffer,
            1 + mem::size_of::<i64>(),
        );
        // This function is called in notify_advance_grant_if_safe(), which is a long
        // function. During this call, the socket might close, causing the following write_to_socket
        // to fail. Consider a failure here a soft failure and update the federate's status.
        let mut error_occurred = false;
        {
            let mut locked_rti = _f_rti.lock().unwrap();
            let mut enclaves = locked_rti.enclaves();
            // FIXME:: Replace "as usize" properly.
            let fed: &Federate = &enclaves[fed_id as usize];
            let mut e = fed.e();
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
            let mut locked_rti = _f_rti.lock().unwrap();
            // FIXME:: Replace "as usize" properly.
            let mut_fed: &mut Federate = &mut locked_rti.enclaves()[fed_id as usize];
            let mut enclave = mut_fed.enclave();
            if error_occurred {
                enclave.set_state(FedState::NOT_CONNECTED);
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
        let mut locked_rti = _f_rti.lock().unwrap();
        let enclaves = locked_rti.enclaves();
        let idx: usize = fed_id.into();
        let fed: &Federate = &enclaves[idx];
        let mut e = fed.e();
        let upstreams = e.upstream();
        for j in upstreams {
            // FIXME:: Replace "as usize" properly.
            let upstream = &enclaves[*j as usize];

            // Ignore this federate if it has resigned.
            if upstream.e().state() == NOT_CONNECTED {
                continue;
            }
            // To handle cycles, need to create a boolean array to keep
            // track of which upstream federates have been visited.
            // FIXME:: Replace "as usize" properly.
            let mut visited = vec![false; number_of_enclaves as usize];

            // Find the (transitive) next event tag upstream.
            let upstream_next_event = Self::transitive_next_event(
                &enclaves,
                upstream.e(),
                upstream.e().next_event(),
                &mut visited,
                start_time,
            );
            // If these tags are equal, then
            // a TAG or PTAG should have already been granted,
            // in which case, another will not be sent. But it
            // may not have been already granted.
            if Tag::lf_tag_compare(&upstream_next_event, &tag) >= 0 {
                Self::notify_provisional_tag_advance_grant(
                    _f_rti.clone(),
                    fed_id,
                    number_of_enclaves,
                    tag.clone(),
                    start_time,
                );
            }
        }
    }

    fn notify_downstream_advance_grant_if_safe(e: &mut Enclave, visited: Vec<bool>) {
        // visited[e.id()] = true;
        // for i in 0..e.num_downstream() {
        //     enclave_t* downstream = _e_rti->enclaves[e->downstream[i]];
        //     if (visited[downstream.id()]) continue;
        //     notify_advance_grant_if_safe(downstream);
        //     notify_downstream_advance_grant_if_safe(downstream, visited);
        // }
    }
}
