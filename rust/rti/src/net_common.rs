pub enum MsgType {
    FED_IDS,
    TIMESTAMP,
    P2P_SENDING_FED_ID,
    P2P_TAGGED_MESSAGE,
    ACK,
}

impl MsgType {
    pub fn to_byte(&self) -> u8 {
        match self {
            MsgType::FED_IDS => 1,
            MsgType::TIMESTAMP => 2,
            MsgType::P2P_SENDING_FED_ID => 15,
            MsgType::P2P_TAGGED_MESSAGE => 17,
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

impl ErrType {
    pub fn to_byte(&self) -> u8 {
        match self {
            ErrType::FEDERATION_ID_DOES_NOT_MATCH => 1,
            ErrType::FEDERATE_ID_IN_USE => 2,
            ErrType::FEDERATE_ID_OUT_OF_RANGE => 3,
            ErrType::UNEXPECTED_MESSAGE => 4,
            ErrType::WRONG_SERVER => 5,
            ErrType::HMAC_DOES_NOT_MATCH => 6,
        }
    }
}
