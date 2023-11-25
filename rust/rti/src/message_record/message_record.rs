/**
 * @file message_record.rs
 * @author Soroush Bateni (soroush@berkeley.edu)
 * @author Chanhee Lee (..)
 * @author Hokeun Kim (hkim501@asu.edu)
 * @brief Record-keeping for in-transit messages.
 * @version 0.1
 * @date 2022-06-02
 *
 * @copyright (c) 2023, The University of California at Berkeley.
 * License in [BSD 2-clause](..)
 */
use priority_queue::PriorityQueue;

use crate::tag::{Instant, Tag};

/**
 * @brief Queue to keep a record of in-transit messages.
 *
 */
pub struct InTransitMessageRecordQueue {
    main_queue: PriorityQueue<Tag, usize>,     // The primary queue.
    transfer_queue: PriorityQueue<Tag, usize>, // Queue used for housekeeping.
}

impl InTransitMessageRecordQueue {
    pub fn new() -> InTransitMessageRecordQueue {
        InTransitMessageRecordQueue {
            main_queue: PriorityQueue::with_capacity(10),
            transfer_queue: PriorityQueue::with_capacity(10),
        }
    }

    pub fn main_queue(&mut self) -> &mut PriorityQueue<Tag, usize> {
        &mut self.main_queue
    }

    pub fn transfer_queue(&mut self) -> &mut PriorityQueue<Tag, usize> {
        &mut self.transfer_queue
    }
}

pub struct MessageRecord {}

impl MessageRecord {
    /**
     * @brief Get the minimum tag of all currently recorded in-transit messages.
     *
     * @param queue The queue to search in (of type `in_transit_message_record_q`).
     * @return tag_t The minimum tag of all currently recorded in-transit messages. Return `FOREVER_TAG` if the queue is empty.
     */
    pub fn get_minimum_in_transit_message_tag(
        queue: &mut InTransitMessageRecordQueue,
        start_time: Instant,
    ) -> Tag {
        let mut minimum_tag = Tag::forever_tag();

        let mut main_queue = queue.main_queue();
        // TODO: let mut transfer_queue = queue.transfer_queue();
        while !main_queue.is_empty() {
            match main_queue.peek() {
                Some(mut head_of_in_transit_messages) => {
                    // The message record queue is ordered according to the `time` field, so we need to check
                    // all records with the minimum `time` and find those that have the smallest tag.
                    let mut head_tag = head_of_in_transit_messages.0.clone();
                    if Tag::lf_tag_compare(&mut head_tag, &mut minimum_tag) <= 0 {
                        minimum_tag = head_tag.clone();
                    } else if head_tag.time() > minimum_tag.time() {
                        break;
                    }
                }
                None => {
                    println!("Failed to peek an item from a main queue.")
                }
            }

            // Add the head to the transfer queue.
            match main_queue.pop() {
                Some(head) => {
                    // TODO: transfer_queue.push(head.0, head.1);
                }
                None => {
                    println!("Failed to pop an item from a main queue.");
                }
            }
        }

        if !main_queue.is_empty() {
            match main_queue.peek() {
                Some(head_of_in_transit_messages) => {
                    let head_tag = head_of_in_transit_messages.0.clone();
                    println!(
                        "RTI: Minimum tag of all in-transit messages: ({},{})",
                        head_tag.time() - start_time,
                        head_tag.microstep()
                    );
                }
                None => {
                    println!("Failed to peek an item from a main queue.")
                }
            }
        }
        minimum_tag
    }
}
