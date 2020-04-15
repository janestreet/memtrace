(** Generate and parse Memtrace allocation traces *)


(** If the MEMTRACE environment variable is set,
    begin tracing to the file it specifies *)
val trace_if_requested : ?sampling_rate:float -> unit -> unit

(** Unconditionally begin tracing to the specified file *)
val trace_until_exit : sampling_rate:float -> filename:string -> unit

(** Tracing can also be manually started and stopped. *)

type tracer

(** Manually start tracing *)
val start_tracing : sampling_rate:float -> filename:string -> tracer

(** Manually stop tracing *)
val stop_tracing : tracer -> unit


(** The remainder of this module consists of functions for parsing
   traces generated using the above *)

type trace
val open_trace : filename:string -> trace

type trace_info = {
  sample_rate : float;
  word_size : int;               (* Either 32 or 64 *)
  executable_name : string;
  host_name : string;
  ocaml_runtime_params : string; (* See Sys.runtime_parameters *)
  pid : Int64.t;
  start_time : Int64.t;          (* Microseconds since Epoch *)
  file_size : Int64.t;           (* Bytes *)
}
val trace_info : trace -> trace_info

type obj_id = private int
type location_code = private int

type location = {
  filename : string;
  line : int;
  start_char : int;
  end_char : int;
  defname : string;
}

val lookup_location : trace -> location_code -> location list

type event =
  | Alloc of {
    (* An identifier for this allocation, used to refer to it in other events.
       These identifiers are generated in allocation order. *)
    obj_id : obj_id;
    (* Length of the sampled allocation, in words, not including header word *)
    length : int;
    (* Number of samples made in this allocation. At least 1. *)
    nsamples : int;
    (* Whether this object was initially allocated on the major heap *)
    is_major : bool;
    (* Backtrace of the allocation.
       The backtrace elements are stored in order from caller to callee.
       The first element is the main entrypoint and the last is the allocation.

       NB: this is a mutable buffer, reused between events.
       Entries at indices beyond [backtrace_length - 1] are not meaningful.
       If you want to store backtraces, you must copy them using:
       [Array.sub backtrace_buffer 0 backtrace_length]. *)
    backtrace_buffer : location_code array;
    backtrace_length : int;
    (* A prefix of this length has not changed since the last event *)
    common_prefix : int;
  }
  | Promote of obj_id
  | Collect of obj_id

(** Microseconds since program start.
    Can be converted to absolute time by adding [trace_info.start_time] *)
type timestamp = Int64.t

(** Iterate over the events in a trace file.
    If backtraces are not necessary, parsing can be sped up by setting parse_backtraces:false. *)
val iter_trace : trace -> ?parse_backtraces:bool -> (timestamp -> event -> unit) -> unit

val close_trace : trace -> unit

(** A hashtable type, keyed by obj_id *)
module IdTbl : Hashtbl.SeededS with type key = obj_id
