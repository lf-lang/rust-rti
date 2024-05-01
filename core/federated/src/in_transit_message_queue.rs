/**
 * @file in_transit_message_queue.rs
 * @author Byeonggil Jun
 * @author Edward A. Lee
 * @author Chanhee Lee
 * @copyright (c) 2023-2024, The University of California at Berkeley
 * License in [BSD 2-clause](https://github.com/lf-lang/reactor-c/blob/main/LICENSE.md)
 *
 * @brief In-transit message queue that uses a priority queue with tags for sorting.
 */
use priority_queue::PriorityQueue;

use crate::tag::Tag;
use crate::FederateInfo;
use crate::RTIRemote;

use std::sync::{Arc, RwLock};

#[derive(PartialEq)]
pub struct InTransitMessageQueue {
    queue: PriorityQueue<Tag, usize>,
}

impl InTransitMessageQueue {
    pub fn new() -> InTransitMessageQueue {
        InTransitMessageQueue {
            queue: PriorityQueue::with_capacity(10),
        }
    }

    pub fn insert_if_no_match_tag(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16, t: Tag) {
        match Self::find_with_tag(_f_rti.clone(), fed_id, t.clone()) {
            Some((_t, _p)) => {}
            None => {
                Self::insert_tag(_f_rti, fed_id, t);
            }
        }
    }

    fn find_with_tag(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16, t: Tag) -> Option<(Tag, usize)> {
        let locked_rti = _f_rti.read().unwrap();
        let idx: usize = fed_id.into();
        let fed = &locked_rti.base().scheduling_nodes()[idx];
        let q = fed.in_transit_message_queue();
        match q.queue().get(&t) {
            Some((tag, priority)) => Some((tag.clone(), priority.clone())),
            None => None,
        }
    }

    fn insert_tag(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16, t: Tag) {
        let mut locked_rti = _f_rti.write().unwrap();
        let idx: usize = fed_id.into();
        let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
        let q = fed.in_transit_message_queue_mut();
        q.queue_mut().push(t, 0);
    }

    pub fn remove_up_to(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16, t: Tag) {
        loop {
            let head = Self::peek_tag(_f_rti.clone(), fed_id);
            if Tag::lf_tag_compare(&head, &Tag::forever_tag()) >= 0
                || Tag::lf_tag_compare(&head, &t) > 0
            {
                break;
            }
            Self::pop(_f_rti.clone(), fed_id);
        }
    }

    pub fn peek_tag(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16) -> Tag {
        let locked_rti = _f_rti.read().unwrap();
        let idx: usize = fed_id.into();
        let fed = &locked_rti.base().scheduling_nodes()[idx];
        let pqueue_tag = fed.in_transit_message_queue();
        if pqueue_tag.queue().len() == 0 {
            return Tag::forever_tag();
        }
        match pqueue_tag.queue().peek() {
            Some(node) => return node.0.clone(),
            None => {
                return Tag::forever_tag();
            }
        }
    }

    fn queue(&self) -> &PriorityQueue<Tag, usize> {
        &self.queue
    }

    fn pop(_f_rti: Arc<RwLock<RTIRemote>>, fed_id: u16) {
        let mut locked_rti = _f_rti.write().unwrap();
        let idx: usize = fed_id.into();
        let fed: &mut FederateInfo = &mut locked_rti.base_mut().scheduling_nodes_mut()[idx];
        let q = fed.in_transit_message_queue_mut();
        match q.queue_mut().pop() {
            Some(..) => {}
            None => {
                println!("Failed to pop an item from the given priority queue.");
            }
        }
    }

    fn queue_mut(&mut self) -> &mut PriorityQueue<Tag, usize> {
        &mut self.queue
    }
}
