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

/**
 * Size of the buffer used for messages sent between federates.
 * This is used by both the federates and the rti, so message lengths
 * should generally match.
 */
pub const FED_COM_BUFFER_SIZE: usize = 256;

pub enum MsgType {
    FED_IDS,
    TIMESTAMP,
    P2P_SENDING_FED_ID,
    P2P_TAGGED_MESSAGE,
    NEIGHBOR_STRUCTURE,
    UDP_PORT,
    ACK,
}

impl MsgType {
    pub fn to_byte(&self) -> u8 {
        match self {
            MsgType::FED_IDS => 1,
            MsgType::TIMESTAMP => 2,
            MsgType::P2P_SENDING_FED_ID => 15,
            MsgType::P2P_TAGGED_MESSAGE => 17,
            MsgType::NEIGHBOR_STRUCTURE => 24,
            MsgType::UDP_PORT => 254,
            MsgType::ACK => 255,
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
