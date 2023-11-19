/**
 * @file
 * @author Edward A. Lee (eal@berkeley.edu)
 * @author Soroush Bateni (soroush@utdallas.edu)
 * @author Erling Jellum (erling.r.jellum@ntnu.no)
 * @author Chadlia Jerad (chadlia.jerad@ensi-uma.tn)
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @copyright (c) 2020-2023, The University of California at Berkeley
 * License in [BSD 2-clause](..)
 * @brief Declarations for runtime infrastructure (RTI) for distributed Lingua Franca programs.
 * This file extends enclave.h with RTI features that are specific to federations and are not
 * used by scheduling enclaves.
 */

////////////////  Type definitions

/**
 * Time instant. Both physical and logical times are represented
 * using this typedef.
 */
type Instant = i64;

/**
 * Interval of time.
 */
pub type Interval = std::option::Option<i64>;

/**
 * Microstep instant.
 */
type Microstep = u32;

/**
 * A tag is a time, microstep pair.
 */
pub struct TAG {
    time: Instant,
    microstep: Microstep,
}

////////////////  Functions

impl TAG {
    pub fn new() -> TAG {
        TAG {
            time: 0,
            microstep: 0,
        }
    }
}
