#!/bin/bash

#############################################################
# Description:                                              #
# This script is made for user convenience                  #
# and can be used to generate HTML reports of code coverage #
# when unit test cases are executed.                        #
#############################################################

# Prerequities
# 1. grcov <== cargo install grcov
# 2. llvm-tools-preview <== rustup component add llvm-tools-preview

rm -rf ./target/coverage
rm -rf cargo-test-*

CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw' cargo test

grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage
