/**
 * @file
 * @author Edward A. Lee (eal@berkeley.edu)
 * @author Soroush Bateni (soroush@utdallas.edu)
 * @author Erling Jellum (erling.r.jellum@ntnu.no)
 * @author Chadlia Jerad (chadlia.jerad@ensi-uma.tn)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @author Chanhee Lee (..)
 * @copyright (c) 2020-2023, The University of California at Berkeley
 * License in [BSD 2-clause](..)
 * @brief Declarations for runtime infrastructure (RTI) for distributed Lingua Franca programs.
 * This file extends enclave.h with RTI features that are specific to federations and are not
 * used by scheduling enclaves.
 */
use crate::tag::{Instant, Microstep};
/**
 * Size of the buffer used for messages sent between federates.
 * This is used by both the federates and the rti, so message lengths
 * should generally match.
 */
pub const FED_COM_BUFFER_SIZE: usize = 256;

/**
 * Delay the start of all federates by this amount.
 * FIXME: More.
 * FIXME: Should use the latency estimates that were
 * acquired during initial clock synchronization.
 */
pub const DELAY_START: i64 = 1;

/**
 * Byte indicating a federate's reply to a MSG_TYPE_STOP_REQUEST that was sent
 * by the RTI. The payload is a proposed stop tag that is at least as large
 * as the one sent to the federate in a MSG_TYPE_STOP_REQUEST message.
 *
 * The next 8 bytes will be the timestamp.
 * The next 4 bytes will be the microstep.
 */
pub const MSG_TYPE_STOP_REQUEST_LENGTH: usize =
    1 + std::mem::size_of::<Instant>() + std::mem::size_of::<Microstep>();
// #[macro_export]
// macro_rules! ENCODE_STOP_GRANTED {
//     (buffer, time, microstep) => {
//         buffer[0] = MsgType::STOP_GRANTED;
//         NetUtil::encode_int64(time, &buffer, 1);
//         assert(microstep >= 0);
//         NetUtil::encode_int32(microstep as i32, &buffer, 1 + std::mem::size_of::<Instant>());
//     }
// }

/**
 * Byte sent by the RTI indicating that the stop request from some federate
 * has been granted. The payload is the tag at which all federates have
 * agreed that they can stop.
 * The next 8 bytes will be the time at which the federates will stop. *
 * The next 4 bytes will be the microstep at which the federates will stop..
 */
pub const MSG_TYPE_STOP_GRANTED_LENGTH: usize =
    1 + std::mem::size_of::<Instant>() + std::mem::size_of::<Microstep>();

pub enum MsgType {
    IGNORE,
    FED_IDS,
    TIMESTAMP,
    TAGGED_MESSAGE,
    NEXT_EVENT_TAG,
    PROVISIONAL_TAG_ADVANCE_GRANT,
    LOGICAL_TAG_COMPLETE,
    STOP_REQUEST,
    STOP_GRANTED,
    ADDRESS_QUERY,
    P2P_SENDING_FED_ID,
    P2P_TAGGED_MESSAGE,
    NEIGHBOR_STRUCTURE,
    UDP_PORT,
    ACK,
}

impl MsgType {
    pub fn to_byte(&self) -> u8 {
        match self {
            MsgType::IGNORE => 0,
            MsgType::FED_IDS => 1,
            MsgType::TIMESTAMP => 2,
            MsgType::TAGGED_MESSAGE => 5,
            MsgType::NEXT_EVENT_TAG => 6,
            MsgType::PROVISIONAL_TAG_ADVANCE_GRANT => 8,
            MsgType::LOGICAL_TAG_COMPLETE => 9,
            MsgType::STOP_REQUEST => 10,
            MsgType::STOP_GRANTED => 12,
            MsgType::ADDRESS_QUERY => 13,
            MsgType::P2P_SENDING_FED_ID => 15,
            MsgType::P2P_TAGGED_MESSAGE => 17,
            MsgType::NEIGHBOR_STRUCTURE => 24,
            MsgType::UDP_PORT => 254,
            MsgType::ACK => 255,
        }
    }

    pub fn to_msg_type(val: u8) -> MsgType {
        match val {
            2 => MsgType::TIMESTAMP,
            5 => MsgType::TAGGED_MESSAGE,
            6 => MsgType::NEXT_EVENT_TAG,
            8 => MsgType::PROVISIONAL_TAG_ADVANCE_GRANT,
            9 => MsgType::LOGICAL_TAG_COMPLETE,
            10 => MsgType::STOP_REQUEST,
            12 => MsgType::STOP_GRANTED,
            13 => MsgType::ADDRESS_QUERY,
            _ => MsgType::IGNORE,
        }
    }
}

pub enum ErrType {
    FEDERATION_ID_DOES_NOT_MATCH,
    FEDERATE_ID_IN_USE,
    FEDERATE_ID_OUT_OF_RANGE,
    UNEXPECTED_MESSAGE,
    WRONG_SERVER,
    HMAC_DOES_NOT_MATCH,
}
