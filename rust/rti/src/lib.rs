/**
 * @file
 * @author Chanhee Lee (chanheel@asu.edu)
 * @author Hokeun Kim (hokeun@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
mod constants;
mod federate_info;
mod in_transit_message_queue;
mod net_common;
mod net_util;
mod rti_common;
mod rti_remote;
mod server;
mod tag;
mod trace;

use std::error::Error;

use crate::constants::*;
use crate::federate_info::*;
use crate::net_common::SocketType;
use crate::rti_common::*;
use crate::rti_remote::*;
use crate::trace::Trace;

use server::Server;

const RTI_TRACE_FILE_NAME: &str = "rti.lft";

#[derive(PartialEq, PartialOrd, Clone, Debug)]
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

pub fn process_args(rti: &mut RTIRemote, argv: &[String]) -> Result<(), &'static str> {
    let mut idx = 1;
    let argc = argv.len();
    while idx < argc {
        let arg = argv[idx].as_str();
        if arg == "-i" || arg == "--id" {
            if argc < idx + 2 {
                println!("--id needs a string argument.");
                usage(argc, argv);
                return Err("Fail to handle id option");
            }
            idx += 1;
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
            rti.base_mut()
                .set_number_of_scheduling_nodes(num_federates.try_into().unwrap()); // FIXME: panic if the converted value doesn't fit
            println!(
                "RTI: Number of federates: {}",
                rti.base().number_of_scheduling_nodes()
            );
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
                    if parsed_value >= u16::MAX {
                        println!(
                            "--port needs a short unsigned integer argument ( < {}).",
                            u16::MAX
                        );
                        usage(argc, argv);
                        return Err("Fail to handle port option");
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
            idx += process_clock_sync_args(rti, argc - idx, &argv[idx..]);
        } else if arg == "-t" || arg == "--tracing" {
            rti.base_mut().set_tracing_enabled(true);
        } else if arg == " " {
            // Tolerate spaces
        } else {
            println!("Unrecognized command-line argument: {}", arg);
            usage(argc, argv);
            return Err("Invalid argument");
        }
        idx += 1;
    }
    if rti.base().number_of_scheduling_nodes() == 0 {
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
    println!("  -t, --tracing Turn on tracing.");

    println!("Command given:");
    let mut idx = 0;
    while idx < argc {
        println!("{} ", argv[idx]);
        idx += 1;
    }
}

/**
 * Process command-line arguments related to clock synchronization. Will return
 * the last read position of argv if all related arguments are parsed or an
 * invalid argument is read.
 */
fn process_clock_sync_args(rti: &mut RTIRemote, argc: usize, argv: &[String]) -> usize {
    for mut i in 0..argc {
        let arg = argv[i].as_str();
        if arg == "off" {
            rti.set_clock_sync_global_status(ClockSyncStat::ClockSyncOff);
            println!("RTI: Clock sync: off");
        } else if arg == "init" || arg == "initial" {
            rti.set_clock_sync_global_status(ClockSyncStat::ClockSyncInit);
            println!("RTI: Clock sync: init");
        } else if arg == "on" {
            rti.set_clock_sync_global_status(ClockSyncStat::ClockSyncOn);
            println!("RTI: Clock sync: on");
        } else if arg == "period" {
            if rti.clock_sync_global_status() != ClockSyncStat::ClockSyncOn {
                println!("[ERROR] clock sync period can only be set if --clock-sync is set to on.");
                usage(argc, argv);
                i += 1;
                continue; // Try to parse the rest of the arguments as clock sync args.
            } else if argc < i + 2 {
                println!("[ERROR] clock sync period needs a time (in nanoseconds) argument.");
                usage(argc, argv);
                continue;
            }
            i += 1;
            match argv[i].as_str().parse::<u64>() {
                Ok(period_ns) => {
                    if period_ns == 0 || period_ns == u64::MAX {
                        println!("[ERROR] clock sync period value is invalid.");
                        continue; // Try to parse the rest of the arguments as clock sync args.
                    }
                    rti.set_clock_sync_period_ns(period_ns);
                    println!("RTI: Clock sync period: {}", rti.clock_sync_period_ns());
                }
                Err(_) => {
                    println!("Failed to parse clock sync period.");
                }
            }
        } else if argv[i] == "exchanges-per-interval" {
            if rti.clock_sync_global_status() != ClockSyncStat::ClockSyncOn
                && rti.clock_sync_global_status() != ClockSyncStat::ClockSyncInit
            {
                println!("[ERROR] clock sync exchanges-per-interval can only be set if\n--clock-sync is set to on or init.");
                usage(argc, argv);
                continue; // Try to parse the rest of the arguments as clock sync args.
            } else if argc < i + 2 {
                println!("[ERROR] clock sync exchanges-per-interval needs an integer argument.");
                usage(argc, argv);
                continue; // Try to parse the rest of the arguments as clock sync args.
            }
            i += 1;
            let exchanges: u32 = 10;
            if exchanges == 0 || exchanges == u32::MAX || exchanges == u32::MIN {
                println!("[ERROR] clock sync exchanges-per-interval value is invalid.");
                continue; // Try to parse the rest of the arguments as clock sync args.
            }
            rti.set_clock_sync_exchanges_per_interval(exchanges); // FIXME: Loses numbers on 64-bit machines
            println!(
                "RTI: Clock sync exchanges per interval: {}",
                rti.clock_sync_exchanges_per_interval()
            );
        } else if arg == " " {
            // Tolerate spaces
            continue;
        } else {
            // Either done with the clock sync args or there is an invalid
            // character. In  either case, let the parent function deal with
            // the rest of the characters;
            return i;
        }
    }
    argc
}

pub fn initialize_federates(rti: &mut RTIRemote) {
    if rti.base().tracing_enabled() {
        let _lf_number_of_workers = rti.base().number_of_scheduling_nodes();
        rti.base_mut()
            .set_trace(Trace::trace_new(RTI_TRACE_FILE_NAME));
        Trace::start_trace(
            rti.base_mut().trace(),
            _lf_number_of_workers.try_into().unwrap(),
        );
        println!("Tracing the RTI execution in {} file.", RTI_TRACE_FILE_NAME);
    }

    for i in 0..rti.base().number_of_scheduling_nodes() {
        let mut federate = FederateInfo::new();
        // FIXME: Handle "as u16" properly.
        initialize_federate(&mut federate, i as u16);
        let scheduling_nodes: &mut Vec<FederateInfo> = rti.base_mut().scheduling_nodes_mut();
        scheduling_nodes.insert(i as usize, federate);
    }
}

fn initialize_federate(fed: &mut FederateInfo, id: u16) {
    let enclave = fed.enclave_mut();
    enclave.initialize_scheduling_node(id);
    // TODO: fed.set_in_transit_message_tags();
    // TODO: fed.set_server_ip_addr();
}

pub fn start_rti_server(_f_rti: &mut RTIRemote) -> Result<Server, Box<dyn Error>> {
    // TODO: _lf_initialize_clock();
    let server = Server::create_rti_server(_f_rti, _f_rti.user_specified_port(), SocketType::TCP);
    if _f_rti.clock_sync_global_status() >= ClockSyncStat::ClockSyncOn {
        let final_tcp_port = u16::from(_f_rti.final_port_tcp());
        Server::create_rti_server(_f_rti, final_tcp_port + 1, SocketType::UDP);
    }
    Ok(server)
}

/**
 * Initialize the RTI instance.
 */
pub fn initialize_rti() -> RTIRemote {
    RTIRemote::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_int_positive() {
        assert!(ClockSyncStat::ClockSyncOff.to_int() == 0);
        assert!(ClockSyncStat::ClockSyncInit.to_int() == 1);
        assert!(ClockSyncStat::ClockSyncOn.to_int() == 2);
    }

    #[test]
    fn test_process_args_option_n_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
    }

    #[test]
    fn test_process_args_option_number_of_federates_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("--number_of_federates"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
    }

    #[test]
    fn test_process_args_option_n_empty_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        assert!(process_args(&mut rti, &args) == Err("Fail to handle number_of_federates option"));
    }

    #[test]
    fn test_process_args_option_n_zero_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("0"));
        assert!(process_args(&mut rti, &args) == Err("Fail to handle number_of_federates option"));
    }

    #[test]
    fn test_process_args_option_n_max_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(i64::MAX.to_string());
        assert!(process_args(&mut rti, &args) == Err("Fail to handle number_of_federates option"));
    }

    #[test]
    fn test_process_args_option_n_string_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("invalid_number_of_federates"));
        assert!(process_args(&mut rti, &args) == Err("Fail to parse a string to i64"));
    }

    #[test]
    fn test_process_args_option_i_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-i"));
        let federation_id = String::from("test_federation_id");
        args.push(federation_id.clone());
        args.push(String::from("-n"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
        assert!(rti.federation_id() == federation_id);
    }

    #[test]
    fn test_process_args_option_id_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("--id"));
        let federation_id = String::from("test_federation_id");
        args.push(federation_id.clone());
        args.push(String::from("-n"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
        assert!(rti.federation_id() == federation_id);
    }

    #[test]
    fn test_process_args_option_i_empty_string_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("--id"));
        assert!(process_args(&mut rti, &args) == Err("Fail to handle id option"));
    }

    #[test]
    fn test_process_args_option_p_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("-p"));
        args.push(String::from("15045"));
        assert!(process_args(&mut rti, &args) == Ok(()));
    }

    #[test]
    fn test_process_args_option_p_empty_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("-p"));
        assert!(process_args(&mut rti, &args) == Err("Fail to handle port option"));
    }

    #[test]
    fn test_process_args_option_p_max_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("-p"));
        args.push(u16::MAX.to_string());
        assert!(process_args(&mut rti, &args) == Err("Fail to handle port option"));
    }

    #[test]
    fn test_process_args_option_p_string_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("-p"));
        args.push(String::from("port"));
        assert!(process_args(&mut rti, &args) == Err("Fail to parse a string to u16"));
    }

    #[test]
    fn test_process_args_option_c_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("-c"));
        args.push(String::from("off"));
        assert!(process_args(&mut rti, &args) == Ok(()));
    }

    #[test]
    fn test_process_args_option_clock_sync_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("--clock_sync"));
        args.push(String::from("off"));
        assert!(process_args(&mut rti, &args) == Ok(()));
    }

    #[test]
    fn test_process_args_option_c_empty_value_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("-c"));
        assert!(process_args(&mut rti, &args) == Err("Fail to handle clock_sync option"));
    }

    #[test]
    fn test_process_args_space_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from(" "));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
    }

    #[test]
    fn test_process_args_unrecognized_command_line_negative() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        args.push(String::from("unrecognized_command-line_argument"));
        assert!(process_args(&mut rti, &args) == Err("Invalid argument"));
    }

    #[test]
    fn test_usage_positive() {
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        usage(args.len(), &args);
    }

    #[test]
    fn test_initialize_federates_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
        initialize_federates(&mut rti);
    }

    #[test]
    fn test_start_rti_server_positive() {
        let mut rti = initialize_rti();
        let mut args: Vec<String> = Vec::new();
        args.push(String::from("target/debug/rti"));
        args.push(String::from("-n"));
        args.push(String::from("2"));
        assert!(process_args(&mut rti, &args) == Ok(()));
        initialize_federates(&mut rti);
        let _ = start_rti_server(&mut rti);
    }
}
