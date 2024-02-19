/**
 * @file
 * @author Edward A. Lee
 * @author Chanhee Lee
 *
 * @section LICENSE
Copyright (c) 2020, The University of California at Berkeley and TU Dresden

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 * @section DESCRIPTION
 * Definitions of tracepoint events for use with the C code generator and any other
 * code generator that uses the C infrastructure (such as the Python code generator).
 *
 * See: https://www.lf-lang.org/docs/handbook/tracing?target=c
 *
 * The trace file is named trace.lft and is a binary file with the following format:
 *
 * Header:
 * * instant_t: The start time. This is both the starting physical time and the starting logical time.
 * * int: Size N of the table mapping pointers to descriptions.
 * This is followed by N records each of which has:
 * * A pointer value (the key).
 * * A null-terminated string (the description).
 *
 * Traces:
 * A sequence of traces, each of which begins with an int giving the length of the trace
 * followed by binary representations of the trace_record struct written using fwrite().
 */
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, RwLock};

use crate::tag::{Instant, Interval, Microstep, Tag};
use crate::RTIRemote;

use zerocopy::AsBytes;

type Void = i64;

const TRACE_BUFFER_CAPACITY: u32 = 2048;

#[derive(Clone, Copy, Debug)]
pub enum TraceEvent {
    ReceiveAdrAd,
    ReceiveAdrQr,
    ReceiveFedId,
    ReceiveNet,
    ReceiveLtc,
    ReceivePortAbs,
    ReceiveResign,
    ReceiveStopReq,
    ReceiveStopReqRep,
    ReceiveTaggedMsg,
    ReceiveTimestamp,
    ReceiveUnidentified,
    SendAck,
    SendPTag,
    SendReject,
    SendStopGrn,
    SendStopReq,
    SendTag,
    SendTaggedMsg,
    SendTimestamp,
}

impl TraceEvent {
    pub fn to_value(&self) -> u64 {
        match self {
            TraceEvent::ReceiveAdrAd => 51,
            TraceEvent::ReceiveAdrQr => 52,
            TraceEvent::ReceiveFedId => 40,
            TraceEvent::ReceiveNet => 35,
            TraceEvent::ReceiveLtc => 36,
            TraceEvent::ReceivePortAbs => 45,
            TraceEvent::ReceiveResign => 44,
            TraceEvent::ReceiveStopReq => 37,
            TraceEvent::ReceiveStopReqRep => 38,
            TraceEvent::ReceiveTaggedMsg => 47,
            TraceEvent::ReceiveTimestamp => 34,
            TraceEvent::ReceiveUnidentified => 53,
            TraceEvent::SendAck => 11,
            TraceEvent::SendPTag => 20,
            TraceEvent::SendReject => 22,
            TraceEvent::SendStopGrn => 18,
            TraceEvent::SendStopReq => 16,
            TraceEvent::SendTag => 21,
            TraceEvent::SendTaggedMsg => 26,
            TraceEvent::SendTimestamp => 13,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TraceRecord {
    event_type: u64,
    pointer: Void, // pointer identifying the record, e.g. to self struct for a reactor.
    src_id: i32,   // The ID number of the source (e.g. worker or federate) or -1 for no ID number.
    dst_id: i32, // The ID number of the destination (e.g. reaction or federate) or -1 for no ID number.
    logical_time: Instant,
    microstep: Microstep,
    physical_time: Instant,
    trigger: Void,
    extra_delay: Instant,
}

impl TraceRecord {
    pub fn new() -> TraceRecord {
        TraceRecord {
            event_type: 0,
            pointer: 0,
            src_id: 0,
            dst_id: 0,
            logical_time: 0,
            microstep: 0,
            physical_time: 0,
            trigger: 0,
            extra_delay: 0,
        }
    }

    pub fn event_type(&self) -> u64 {
        self.event_type
    }

    pub fn pointer(&self) -> Void {
        self.pointer
    }

    pub fn src_id(&self) -> i32 {
        self.src_id
    }

    pub fn dst_id(&self) -> i32 {
        self.dst_id
    }

    pub fn logical_time(&self) -> Instant {
        self.logical_time
    }

    pub fn microstep(&self) -> Microstep {
        self.microstep
    }

    pub fn physical_time(&self) -> Instant {
        self.physical_time
    }

    pub fn trigger(&self) -> Void {
        self.trigger
    }

    pub fn extra_delay(&self) -> Instant {
        self.extra_delay
    }

    pub fn set_event_type(&mut self, event_type: u64) {
        self.event_type = event_type;
    }

    pub fn set_pointer(&mut self, pointer: Void) {
        self.pointer = pointer;
    }

    pub fn set_src_id(&mut self, src_id: i32) {
        self.src_id = src_id;
    }

    pub fn set_dst_id(&mut self, dst_id: i32) {
        self.dst_id = dst_id;
    }

    pub fn set_logical_time(&mut self, logical_time: Instant) {
        self.logical_time = logical_time;
    }

    pub fn set_microstep(&mut self, microstep: Microstep) {
        self.microstep = microstep;
    }

    pub fn set_physical_time(&mut self, physical_time: Instant) {
        self.physical_time = physical_time;
    }

    pub fn set_trigger(&mut self, trigger: Void) {
        self.trigger = trigger;
    }

    pub fn set_extra_delay(&mut self, extra_delay: Instant) {
        self.extra_delay = extra_delay;
    }
}

pub struct Trace {
    /**
     * Array of buffers into which traces are written.
     * When a buffer becomes full, the contents is flushed to the file,
     * which will create a significant pause in the calling thread.
     */
    _lf_trace_buffer: Vec<Vec<TraceRecord>>,
    _lf_trace_buffer_size: Vec<u32>,

    /** The number of trace buffers allocated when tracing starts. */
    _lf_number_of_trace_buffers: usize,

    /** Marker that tracing is stopping or has stopped. */
    _lf_trace_stop: usize,

    /** The file into which traces are written. */
    _lf_trace_file: Option<File>,

    /** The file name where the traces are written*/
    filename: String,

    /** Table of pointers to a description of the object. */
    // TODO: object_description_t _lf_trace_object_descriptions[TRACE_OBJECT_TABLE_SIZE];
    _lf_trace_object_descriptions_size: i32,

    /** Indicator that the trace header information has been written to the file. */
    _lf_trace_header_written: bool,
    // Pointer back to the environment which we are tracing within
    // TODO: env: Option<Environment>,
}

impl Trace {
    // TODO: pub fn trace_new(filename: &str, env: Environment) -> Trace {
    pub fn trace_new(filename: &str) -> Trace {
        Trace {
            _lf_trace_buffer: Vec::new(),
            _lf_trace_buffer_size: Vec::new(),
            _lf_number_of_trace_buffers: 0,
            _lf_trace_stop: 1,
            _lf_trace_file: None,
            filename: String::from(filename),
            _lf_trace_object_descriptions_size: 0,
            _lf_trace_header_written: false,
            // TODO: env: env,
        }
    }

    pub fn lf_trace_buffer(&self) -> &Vec<Vec<TraceRecord>> {
        &self._lf_trace_buffer
    }

    pub fn lf_trace_buffer_mut(&mut self) -> &mut Vec<Vec<TraceRecord>> {
        &mut self._lf_trace_buffer
    }

    pub fn lf_trace_buffer_size(&self) -> &Vec<u32> {
        &self._lf_trace_buffer_size
    }

    pub fn lf_trace_buffer_size_mut(&mut self) -> &mut Vec<u32> {
        &mut self._lf_trace_buffer_size
    }

    pub fn lf_number_of_trace_buffers(&self) -> usize {
        self._lf_number_of_trace_buffers
    }

    pub fn lf_trace_stop(&self) -> usize {
        self._lf_trace_stop
    }

    pub fn lf_trace_file(&self) -> &Option<File> {
        &self._lf_trace_file
    }

    pub fn lf_trace_file_mut(&mut self) -> &mut Option<File> {
        &mut self._lf_trace_file
    }

    pub fn filename(&self) -> &String {
        &self.filename
    }

    pub fn lf_trace_object_descriptions_size(&self) -> i32 {
        self._lf_trace_object_descriptions_size
    }

    pub fn lf_trace_header_written(&self) -> bool {
        self._lf_trace_header_written
    }

    pub fn set_lf_number_of_trace_buffers(&mut self, _lf_number_of_trace_buffers: usize) {
        self._lf_number_of_trace_buffers = _lf_number_of_trace_buffers;
    }

    pub fn set_lf_trace_stop(&mut self, _lf_trace_stop: usize) {
        self._lf_trace_stop = _lf_trace_stop;
    }

    pub fn set_lf_trace_file(&mut self, _lf_trace_file: Option<File>) {
        self._lf_trace_file = _lf_trace_file;
    }

    pub fn set_lf_trace_header_written(&mut self, _lf_trace_header_written: bool) {
        self._lf_trace_header_written = _lf_trace_header_written;
    }

    pub fn start_trace(trace: &mut Trace, _lf_number_of_workers: usize) {
        // FIXME: location of trace file should be customizable.
        match File::create(trace.filename()) {
            Ok(trace_file) => {
                trace.set_lf_trace_file(Some(trace_file));
            }
            Err(e) => {
                println!(
                    "WARNING: Failed to open log file with error code {}. No log will be written.",
                    e
                );
            }
        }
        // Do not write the trace header information to the file yet
        // so that startup reactions can register user-defined trace objects.
        // TODO: write_trace_header();
        trace.set_lf_trace_header_written(false);

        // Allocate an array of arrays of trace records, one per worker thread plus one
        // for the 0 thread (the main thread, or in an single-threaded program, the only
        // thread).
        trace.set_lf_number_of_trace_buffers(_lf_number_of_workers + 1);
        for _i in 0.._lf_number_of_workers {
            trace.lf_trace_buffer_mut().push(Vec::new());
            trace.lf_trace_buffer_size_mut().push(0);
        }

        trace.set_lf_trace_stop(0);
        println!("Started tracing.");
    }

    pub fn stop_trace_locked(trace: &mut Trace, start_time: Instant) -> bool {
        let trace_stopped = trace.lf_trace_stop();
        if trace_stopped + 1 < trace.lf_number_of_trace_buffers() - 1 {
            // Not all federates finish yet. Nothing to do.
            trace.set_lf_trace_stop(trace_stopped + 1);
            return false;
        }
        // In multithreaded execution, thread 0 invokes wrapup reactions, so we
        // put that trace last. However, it could also include some startup events.
        // In any case, the trace file does not guarantee any ordering.
        for i in 1..trace.lf_number_of_trace_buffers() - 1 {
            // Flush the buffer if it has data.
            println!(
                "DEBUG: Trace buffer {} has {} records.\n",
                i,
                trace.lf_trace_buffer_size()[i]
            );
            if trace.lf_trace_buffer_size()[i] > 0 {
                Self::flush_trace_locked(trace, i, start_time);
            }
        }
        if trace.lf_trace_buffer_size()[0] > 0 {
            println!(
                "DEBUG: Trace buffer 0 has {} records.\n",
                trace.lf_trace_buffer_size()[0]
            );
            Self::flush_trace_locked(trace, 0, start_time);
        }
        println!("Stopped tracing.");
        true
    }

    pub fn tracepoint_rti_from_federate(
        trace: &mut Trace,
        event_type: TraceEvent,
        fed_id: u16,
        tag: Tag,
        start_time: Instant,
    ) {
        Self::tracepoint(
            trace,
            event_type,
            // TODO: NULL,
            &Some(tag),    // tag_t* tag,
            fed_id.into(), // int worker (one thread per federate)
            -1,            // int src_id
            fed_id.into(), // int dst_id
            &mut 0,        // instant_t* physical_time (will be generated)
            // TODO: NULL,
            Some(0), // interval_t extra_delay
            true,    // is_interval_start
            start_time,
        );
    }

    pub fn tracepoint_rti_to_federate(
        trace: &mut Trace,
        event_type: TraceEvent,
        fed_id: u16,
        tag: Tag,
        start_time: Instant,
    ) {
        Self::tracepoint(
            trace,
            event_type,
            // TODO: NULL,
            &Some(tag),    // tag_t* tag,
            fed_id.into(), // int worker (one thread per federate)
            -1,            // int src_id
            fed_id.into(), // int dst_id
            &mut 0,        // instant_t* physical_time (will be generated)
            // TODO: NULL,
            Some(0), // interval_t extra_delay
            true,    // is_interval_start
            start_time,
        );
    }

    fn tracepoint(
        trace: &mut Trace,
        event_type: TraceEvent,
        // TODO: void* reactor,
        tag: &Option<Tag>,
        worker: usize,
        src_id: i32,
        dst_id: i32,
        physical_time: &mut Instant,
        // TODO: trigger_t* trigger,
        extra_delay: Interval,
        is_interval_start: bool,
        start_time: Instant,
    ) {
        let mut time;
        if !is_interval_start && *physical_time == 0 {
            time = Tag::lf_time_physical();
            *physical_time = time;
        }

        // TODO: environment_t *env = trace->env;
        // Worker argument determines which buffer to write to.
        let index = worker;

        // Flush the buffer if it is full.
        if trace.lf_trace_buffer_size()[index] >= TRACE_BUFFER_CAPACITY {
            // No more room in the buffer. Write the buffer to the file.
            Self::flush_trace(trace, index, start_time);
        }
        // The above flush_trace resets the write pointer.
        let _lf_trace_buffer_size = trace.lf_trace_buffer_size()[index];
        let i = _lf_trace_buffer_size as usize;

        // Write to memory buffer.
        // Get the correct time of the event
        let buffer_len = trace.lf_trace_buffer_mut()[index].len();
        if buffer_len <= i {
            for _a in 0..(i as usize + 1 - buffer_len) {
                trace.lf_trace_buffer_mut()[index].push(TraceRecord::new());
            }
        }
        let lf_trace_buffer: &mut Vec<TraceRecord> = &mut trace.lf_trace_buffer_mut()[index];
        lf_trace_buffer[i].set_event_type(event_type.to_value());
        lf_trace_buffer[i].set_pointer(0);
        lf_trace_buffer[i].set_src_id(src_id);
        lf_trace_buffer[i].set_dst_id(dst_id);
        // TODO: Handle unwrap() properly.
        let _tag = tag.as_ref().unwrap();
        lf_trace_buffer[i].set_logical_time(_tag.time());
        lf_trace_buffer[i].set_microstep(_tag.microstep());
        // TODO: Enable following code
        // else if (env != NULL) {
        //     trace->_lf_trace_buffer[index][i].logical_time = ((environment_t *)env)->current_tag.time;
        //     trace->_lf_trace_buffer[index][i].microstep = ((environment_t*)env)->current_tag.microstep;
        // }
        lf_trace_buffer[i].set_trigger(0);
        lf_trace_buffer[i].set_extra_delay(extra_delay.unwrap());
        if is_interval_start && *physical_time == 0 {
            time = Tag::lf_time_physical();
            *physical_time = time;
        }
        lf_trace_buffer[i].set_physical_time(*physical_time);
        let lf_trace_buffer_size = _lf_trace_buffer_size + 1;
        let _ = std::mem::replace(
            &mut trace.lf_trace_buffer_size_mut()[index],
            lf_trace_buffer_size,
        );
    }

    fn flush_trace(trace: &mut Trace, worker: usize, start_time: Instant) {
        Self::flush_trace_locked(trace, worker, start_time);
    }

    fn flush_trace_locked(trace: &mut Trace, worker: usize, start_time: Instant) {
        let _lf_trace_buffer_size = trace.lf_trace_buffer_size()[worker];
        if _lf_trace_buffer_size > 0 {
            // If the trace header has not been written, write it now.
            // This is deferred to here so that user trace objects can be
            // registered in startup reactions.
            if !trace.lf_trace_header_written() {
                if Self::write_trace_header(trace, start_time) < 0 {
                    println!("Failed to write trace header. Trace file will be incomplete.");
                    return;
                }
                trace.set_lf_trace_header_written(true);
            }

            // Write first the length of the array.
            Self::write_to_trace_file(
                trace.filename(),
                &_lf_trace_buffer_size.to_le_bytes().to_vec(),
            );

            // Write the contents.
            if worker >= trace.lf_trace_buffer().len() {
                println!(
                    "[WARNING] worker({}) >= trace buffer length({})",
                    worker,
                    trace.lf_trace_buffer().len()
                );
                return;
            }
            for i in 0..trace.lf_trace_buffer_size_mut()[worker] {
                let trace_record = &trace.lf_trace_buffer_mut()[worker][i as usize];
                let encoded_trace_records = vec![
                    trace_record.event_type().as_bytes(),
                    trace_record.pointer().as_bytes(),
                    trace_record.src_id().as_bytes(),
                    trace_record.dst_id().as_bytes(),
                    trace_record.logical_time().as_bytes(),
                    trace_record.microstep().as_bytes(),
                    0.as_bytes(),
                    trace_record.physical_time().as_bytes(),
                    trace_record.trigger().as_bytes(),
                    trace_record.extra_delay().as_bytes(),
                ]
                .concat();
                Self::write_to_trace_file(trace.filename(), &encoded_trace_records);
            }
            trace.lf_trace_buffer_size_mut()[worker] = 0;
        }
    }

    fn write_trace_header(trace: &mut Trace, start_time: Instant) -> i32 {
        // The first item in the header is the start time.
        // This is both the starting physical time and the starting logical time.
        println!(
            "[DEBUG]: Start time written to trace file is {}.\n",
            start_time
        );
        Self::write_to_trace_file(trace.filename(), &start_time.to_le_bytes().to_vec());

        // The next item in the header is the size of the
        // _lf_trace_object_descriptions table.
        Self::write_to_trace_file(
            trace.filename(),
            &trace
                .lf_trace_object_descriptions_size()
                .to_le_bytes()
                .to_vec(),
        );

        // TODO: Implement "Next we write the table".

        trace.lf_trace_object_descriptions_size()
    }

    fn write_to_trace_file(filename: &String, buffer: &Vec<u8>) {
        match OpenOptions::new().write(true).append(true).open(filename) {
            Ok(mut file) => match file.write(buffer) {
                Ok(bytes_written) => {
                    if bytes_written != buffer.len() {
                        println!("WARNING: Access to trace file failed.\n");
                        return;
                    }
                }
                Err(_e) => {
                    println!("Fail to write to the RTI trace file.");
                    return;
                }
            },
            Err(_e) => {
                println!("Fail to open the RTI trace file.");
                return;
            }
        }
    }
    pub fn log_trace(
        rti: Arc<RwLock<RTIRemote>>,
        trace_event: TraceEvent,
        fed_id: u16,
        tag: &Tag,
        start_time: Instant,
        direction: TraceDirection,
    ) {
        let mut locked_rti = rti.write().unwrap();
        let tracing_enabled = locked_rti.base().tracing_enabled();
        let trace = locked_rti.base_mut().trace();
        if tracing_enabled {
            match direction {
                TraceDirection::From => Trace::tracepoint_rti_from_federate(
                    trace,
                    trace_event,
                    fed_id,
                    tag.clone(),
                    start_time,
                ),
                TraceDirection::To => Trace::tracepoint_rti_to_federate(
                    trace,
                    trace_event,
                    fed_id,
                    tag.clone(),
                    start_time,
                ),
            }
        }
    }
}

pub enum TraceDirection {
    From,
    To,
}
