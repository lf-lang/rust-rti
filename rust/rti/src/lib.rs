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

use crate::constants::*;
use crate::federate_rti::*;

use server::Server;

pub struct Config {
    pub ip_v4: String,
    pub port: String,
}

impl Config {
    pub fn build(rti: &mut FederateRTI, argv: &[String]) -> Result<(), &'static str> {
        let mut idx = 1;
        let mut ip_v4 = String::from("0.0.0.0");
        let mut port = String::from("15045");
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
                let mut num_federates: i64;
                match argv[idx].parse::<i64>() {
                    Ok(parsed_value) => {
                        if parsed_value == 0 || parsed_value == i64::MAX || parsed_value == i64::MIN
                        {
                            println!(
                                "--number_of_federates needs a valid positive integer argument."
                            );
                            usage(argc, argv);
                            return Err("Fail to handle number_of_federates option");
                        }
                        num_federates = parsed_value;
                    }
                    Err(e) => {
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
                let mut RTI_port: u16;
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
                        RTI_port = parsed_value;
                    }
                    Err(e) => {
                        return Err("Fail to parse a string to u16");
                    }
                }
                rti.set_port(RTI_port.try_into().unwrap());
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

            // if argv.len() > 2 {
            //     ip_v4 = String::from(&argv[1].clone());
            //     port = String::from(&argv[2].clone());
            // }
        }
        if rti.number_of_enclaves() == 0 {
            println!("--number_of_federates needs a valid positive integer argument.");
            usage(argc, argv);
            return Err("Invalid number of enclaves");
        }
        Ok(())
    }
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
    println!("  -a, --auth Turn on HMAC authentication options.");
    println!("  -t, --tracing Turn on tracing.");

    println!("Command given:");
    let mut idx = 0;
    while idx < argc {
        println!("{} ", argv[idx]);
        idx += 1;
    }
}

pub fn run(rti: &mut FederateRTI) -> Result<(), Box<dyn Error>> {
    let server = Server::new(rti.user_specified_port().to_string());
    server.listen();

    Ok(())
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
fn process_clock_sync_args(argc: i32, argv: &[String]) -> i32 {
    // TODO: implement this function
    0
}

/**
 * Initialize the _RTI instance.
 */
pub fn initialize_RTI() -> FederateRTI {
    FederateRTI::new()
}
