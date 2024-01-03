# Rust RTI: Lingua Franca Runtime Infrastructure (RTI) in Rust Language

This repository contains the Rust code for Lingua Franca's (LF) Runtime Infrastructure (RTI) for federated execution of LF.

> **_Disclaimer_**
>
> This RTI is still a work in progress with unimplemented functionalities; thus, it may not work for certain federated LF programs.
> Please let @chanijjani or @hokeun know if you find any issues when running federated LF programs with this Rust RTI.

## Requirements
- Rust runtime: https://www.rust-lang.org/tools/install

## Quick Start

### Setup Environment (Ubuntu)
1. Update system to the latest.
```
sudo apt update
```
2. Install packages for Rust dependents.
```
sudo apt install -y curl gcc make build-essential
```
3. Install Rust.
```
curl https://sh.rustup.rs -sSf | sh
```
4. Check Rust installation.
```
source ~/.profile
source ~/.cargo/env
rustc -V
```
You can see `rustc 1.xx.0 (...)`.

### Run

1. Download sources from Github
```
https://github.com/hokeun/lf-rust-rti.git
```
2. Change the directory into `lf-rust-rti/rust/rti`, then run the `cargo run` command with options for running the RTI, as shown below.

```
cd lf-rust-rti/rust/rti
cargo run -- -n 2
```

## Current Status

- Passing federated tests (lingua-franca/test/C/src/federated/) with Rust RTI: 
  - DistributedCount.lf, DistributedStop.lf, HelloDistributed.lf, PingPongDistibuted.lf, SimpleFederated.lf, StopAtShutdown.lf
