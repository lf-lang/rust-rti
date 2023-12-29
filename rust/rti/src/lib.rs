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
mod federation_rti;
mod message_record {
    pub mod message_record;
    pub mod rti_pqueue_support;
}
mod net_common;
mod net_util;
mod server;
mod tag;

use std::error::Error;

use crate::constants::*;
use crate::enclave::*;
use crate::federate::*;
use crate::federation_rti::*;

use server::Server;

#[derive(PartialEq, PartialOrd, Clone)]
pub enum ClockSyncStat {
    ClockSyncOff,
    ClockSyncInit,
    ClockSyncOn,
}

impl ClockSyncStat {
    pub fn to_int(&self) -> i32 {
        match self {
            ClockSyncStat::ClockSyncOff => 0,
            ClockSyncStat::ClockSyncInit => 1,
            ClockSyncStat::ClockSyncOn => 2,
        }
    }
}

pub fn process_args(rti: &mut FederationRTI, argv: &[String]) -> Result<(), &'static str> {
    let mut idx = 1;
    let argc = argv.len();
    while idx < argc {
        let arg = argv[idx].as_str();
        // println!("arg = {}", arg); // TODO: Remove this debugging code
        if arg == "-i" || arg == "--id" {
            if argc < idx + 2 {
                println!("--id needs a string argument.");
                usage(argc, argv);
                return Err("Fail to handle id option");
            }
            idx += 1;
            // println!("idx = {}", idx); // TODO: Remove this debugging code
            println!("RTI: Federation ID: {}", arg);
            rti.set_federation_id(argv[idx].clone());
        } else if arg == "-n" || arg == "--number_of_federates" {
            if argc < idx + 2 {
                println!("--number_of_federates needs an integer argument.");
                usage(argc, argv);
                return Err("Fail to handle number_of_federates option");
            }
            idx += 1;
            let num_federates: i64;
            match argv[idx].parse::<i64>() {
                Ok(parsed_value) => {
                    if parsed_value == 0 || parsed_value == i64::MAX || parsed_value == i64::MIN {
                        println!("--number_of_federates needs a valid positive integer argument.");
                        usage(argc, argv);
                        return Err("Fail to handle number_of_federates option");
                    }
                    num_federates = parsed_value;
                }
                Err(_e) => {
                    return Err("Fail to parse a string to i64");
                }
            };
            rti.set_number_of_enclaves(num_federates.try_into().unwrap()); // FIXME: panic if the converted value doesn't fit
            println!("RTI: Number of federates: {}", rti.number_of_enclaves());
        } else if arg == "-p" || arg == "--port" {
            if argc < idx + 2 {
                println!(
                    "--port needs a short unsigned integer argument ( > 0 and < {}).",
                    u16::MAX
                );
                usage(argc, argv);
                return Err("Fail to handle port option");
            }
            idx += 1;
            let rti_port: u16;
            match argv[idx].parse::<u16>() {
                Ok(parsed_value) => {
                    if parsed_value <= 0 || parsed_value >= u16::MAX {
                        println!(
                            "--port needs a short unsigned integer argument ( > 0 and < {}).",
                            u16::MAX
                        );
                        usage(argc, argv);
                        return Err("Fail to handle number_of_federates option");
                    }
                    rti_port = parsed_value;
                }
                Err(_e) => {
                    return Err("Fail to parse a string to u16");
                }
            }
            rti.set_port(rti_port.try_into().unwrap());
        } else if arg == "-c" || arg == "--clock_sync" {
            if argc < idx + 2 {
                println!("--clock-sync needs off|init|on.");
                usage(argc, argv);
                return Err("Fail to handle clock_sync option");
            }
            idx += 1;
            // TODO: idx += process_clock_sync_args();
        } else if arg == " " {
            // Tolerate spaces
            continue;
        } else {
            println!("Unrecognized command-line argument: {}", arg);
            usage(argc, argv);
            return Err("Invalid argument");
        }
        idx += 1;
    }
    if rti.number_of_enclaves() == 0 {
        println!("--number_of_federates needs a valid positive integer argument.");
        usage(argc, argv);
        return Err("Invalid number of enclaves");
    }
    Ok(())
}

fn usage(argc: usize, argv: &[String]) {
    println!("\nCommand-line arguments: ");
    println!("  -i, --id <n>");
    println!("   The ID of the federation that this RTI will control.");
    println!("  -n, --number_of_federates <n>");
    println!("   The number of federates in the federation that this RTI will control.");
    println!("  -p, --port <n>");
    println!("   The port number to use for the RTI. Must be larger than 0 and smaller than {}. Default is {}.", u16::MAX, STARTING_PORT);
    println!("  -c, --clock_sync [off|init|on] [period <n>] [exchanges-per-interval <n>]");
    println!("   The status of clock synchronization for this federate.");
    println!("       - off: Clock synchronization is off.");
    println!("       - init (default): Clock synchronization is done only during startup.");
    println!(
        "       - on: Clock synchronization is done both at startup and during the execution."
    );
    println!("   Relevant parameters that can be set: ");
    println!("       - period <n>(in nanoseconds): Controls how often a clock synchronization attempt is made");
    println!("          (period in nanoseconds, default is 5 msec). Only applies to 'on'.");
    println!("       - exchanges-per-interval <n>: Controls the number of messages that are exchanged for each");
    println!("          clock sync attempt (default is 10). Applies to 'init' and 'on'.");

    println!("Command given:");
    let mut idx = 0;
    while idx < argc {
        println!("{} ", argv[idx]);
        idx += 1;
    }
}

pub fn initialize_federates(rti: &mut FederationRTI) {
    let mut i: u16 = 0;
    while i32::from(i) < rti.number_of_enclaves() {
        let mut federate = Federate::new();
        initialize_federate(&mut federate, i);
        let enclaves: &mut Vec<Federate> = rti.enclaves();
        enclaves.push(federate);
        i += 1;
    }
}

fn initialize_federate(fed: &mut Federate, id: u16) {
    let enclave = fed.enclave();
    enclave.initialize_enclave(id);
    // TODO: fed.set_in_transit_message_tags();
    // TODO: fed.set_server_ip_addr();
}

pub fn start_rti_server(_f_rti: &mut FederationRTI) -> Result<Server, Box<dyn Error>> {
    // TODO: _lf_initialize_clock();
    Ok(Server::create_server(
        _f_rti.user_specified_port().to_string(),
    ))
}

/**
 * Process command-line arguments related to clock synchronization. Will return
 * the last read position of argv if all related arguments are parsed or an
 * invalid argument is read.
 *
 * @param argc: Number of arguments in the list
 * @param argv: The list of arguments as a string
 * @return Current position (head) of argv;
 */
// TODO: implement this function
// fn process_clock_sync_args(_argc: i32, _argv: &[String]) -> i32 {
//     0
// }

/**
 * Initialize the _RTI instance.
 */
pub fn initialize_rti() -> FederationRTI {
    FederationRTI::new()
}
