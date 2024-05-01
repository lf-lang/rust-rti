/**
 * @file
 * @author Hokeun Kim (hokeun@asu.edu)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::env;
use std::process;

fn main() {
    let mut rti = rti::initialize_rti();

    let args: Vec<String> = env::args().collect();
    // dbg!(args);

    rti::process_args(&mut rti, &args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    println!(
        "Starting RTI for {} federates in federation ID {}.",
        rti.base().number_of_scheduling_nodes(),
        rti.federation_id()
    );
    assert!(rti.base().number_of_scheduling_nodes() < u16::MAX.into());

    rti::initialize_federates(&mut rti);

    let server = rti::start_rti_server(&mut rti);
    server
        .expect("Failed to wait for federates")
        .wait_for_federates(rti);
}
