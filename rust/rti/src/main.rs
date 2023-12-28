/**
 * @file
 * @author Hokeun Kim (hkim501@asu.edu)
 * @author Chanhee Lee (..)
 * @copyright (c) 2023, Arizona State University
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use std::env;
use std::process;

fn main() {
    let mut _f_rti = rti::initialize_rti();

    let args: Vec<String> = env::args().collect();
    // dbg!(args);

    rti::process_args(&mut _f_rti, &args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    println!(
        "Starting RTI for {} federates in federation ID {}.",
        _f_rti.number_of_enclaves(),
        _f_rti.federation_id()
    );
    assert!(_f_rti.number_of_enclaves() < u16::MAX.into());

    rti::initialize_federates(&mut _f_rti);

    let server = rti::start_rti_server(&mut _f_rti);
    server
        .expect("Failed to wait for federates")
        .wait_for_federates(_f_rti);
}
