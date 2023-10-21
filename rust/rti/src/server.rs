use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

enum MsgType {
    FED_IDS,
    TIMESTAMP,
}

impl MsgType {
    pub fn to_byte(&self) -> u8 {
        match self {
            MsgType::FED_IDS => 1,
            MsgType::TIMESTAMP => 2,
        }
    }
}

pub struct Server {
    ip_v4: String,
    port: String,
}

impl Server {
    pub fn new(ip_v4: String, port: String) -> Server {
        Server { ip_v4, port }
    }

    pub fn listen(self) {
        let mut address = self.ip_v4;
        address.push(':');
        address.push_str(self.port.as_str());
        let listener = TcpListener::bind(address).unwrap();
        // accept connections and process them, spawning a new thread for each one
        println!("Server listening on port 15045");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("New connection: {}", stream.peer_addr().unwrap());
                    thread::spawn(move || {
                        // connection succeeded
                        Self::handle_client(stream)
                    });
                }
                Err(e) => {
                    println!("Error: {} during stream handling", e);
                    /* connection failed */
                }
            }
        }

        // close the socket server
        drop(listener);
    }

    fn handle_client(mut stream: TcpStream) {
        let mut data = [0 as u8; 50]; // using 50 byte buffer
        while match stream.read(&mut data) {
            Ok(size) => {
                // echo everything!
                for x in data {
                    print!("{:02X?} ", x);
                    // write!("{:X} ", value).expect("Unable to write");
                }
                if data[0] == MsgType::FED_IDS.to_byte() {
                    // LittleEndian::read_u16_into(&bytes, &mut numbers_got);
                    let fed_id = u16::from_le_bytes(data[1..3].try_into().unwrap());
                    println!("fedId: {}", fed_id);
                    let federation_id_length = u8::from_le_bytes(data[3..4].try_into().unwrap());
                    println!("federationIdLength: {}", federation_id_length);
                    match String::from_utf8(data[4..].to_vec()) {
                        Ok(federation_id) => {
                            println!("federationId: {}", federation_id);
                        }
                        Err(e) => {
                            println!("Failed {}", e);
                        }
                    };

                    // let fedId:u16 = BigEndian::read_u16(&mut data[1..2]);
                }
                println!("");
                //stream.write(&data[0..size]).unwrap();
                true
            }
            Err(_) => {
                println!(
                    "An error occurred, terminating connection with {}",
                    stream.peer_addr().unwrap()
                );
                stream.shutdown(Shutdown::Both).unwrap();
                false
            }
        } {}
    }
}
