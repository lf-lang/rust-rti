/**
 * @file
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
mod constants;
mod enclave;
mod federate;
mod federate_rti;
mod net_common;
mod server;

use std::error::Error;

use crate::federate_rti::*;
use server::Server;

pub struct Config {
    pub ip_v4: String,
    pub port: String,
}

impl Config {
    pub fn build(args: &[String]) -> Result<Config, &'static str> {
        let mut ip_v4 = String::from("0.0.0.0");
        let mut port = String::from("15045");
        if args.len() > 2 {
            ip_v4 = String::from(&args[1].clone());
            port = String::from(&args[2].clone());
        }
        Ok(Config { ip_v4, port })
    }
}

pub fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let rti = initialize_RTI();

    let server = Server::new(config.ip_v4, config.port);
    server.listen();

    Ok(())
}

fn initialize_RTI() -> FederateRTI {
    FederateRTI::new()
}
