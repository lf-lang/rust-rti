/**
 * @file
 * @author Soroush Bateni (soroush@berkeley.edu)
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @copyright (c) 2023, The University of California at Berkeley.
 * License in [BSD 2-clause](..)
 * @brief ..
 */

// ********** Priority Queue Support Start
/**
 * @brief Represent an in-transit message.
 *
 */
pub struct InTransitMessageRecord {
    tag: Tag;      // Tag of the in-transit message.
    pos: usize;     // Position in the priority queue.
}
