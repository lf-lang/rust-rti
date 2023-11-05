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

use rti::*;

fn main() {
    let mut rti = rti::initialize_RTI();

    let args: Vec<String> = env::args().collect();
    // dbg!(args);

    Config::build(&mut rti, &args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    if let Err(_e) = rti::run(&mut rti) {}
}
