/**
 * @file
 * @author Soroush Bateni (soroush@berkeley.edu)
 * @author Chanhee Lee (chanheel@asu.edu)
 * @author Hokeun Kim (hokeun@asu.edu)
 * @copyright (c) 2023, The University of California at Berkeley.
 * License in [BSD 2-clause](..)
 * @brief ..
 */
use crate::tag::Tag;

// ********** Priority Queue Support Start
/**
 * @brief Represent an in-transit message.
 *
 */
pub struct InTransitMessageRecord {
    tag: Tag,   // Tag of the in-transit message.
    pos: usize, // Position in the priority queue.
}

impl InTransitMessageRecord {
    pub fn new(tag: Tag, pos: usize) -> InTransitMessageRecord {
        InTransitMessageRecord { tag, pos }
    }

    pub fn tag(&self) -> Tag {
        self.tag.clone()
    }

    pub fn pos(&self) -> usize {
        self.pos
    }
}
