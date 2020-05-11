(** Encoder and decoder for Memtrace traces *)

(** Timestamps are encoded as the number of microseconds since the Unix epoch *)
type timestamp = int64

(** Convert back and forth between the Unix module's float format and timestamps *)
val float_of_timestamp : timestamp -> float
val timestamp_of_float : float -> timestamp

(** Source locations in the traced program *)
type location = {
  filename : string;
  line : int;
  start_char : int;
  end_char : int;
  defname : string;
}
val string_of_location : location -> string

(** Trace events
    Locations are represented as integers in these events *)
type obj_id = private int
type location_code = int
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
val string_of_event : (location_code -> location list) -> event -> string


(** Global trace info *)
type trace_info = {
  sample_rate : float;
  word_size : int;
  executable_name : string;
  host_name : string;
  ocaml_runtime_params : string;
  pid : Int64.t;
  start_time : timestamp;
}


(** Writing traces *)
type trace_writer
val make_writer : Unix.file_descr -> ?getpid:(unit -> int64) -> trace_info -> trace_writer
val put_alloc : trace_writer -> timestamp ->
                length:int -> nsamples:int ->
                is_major:bool ->
                callstack:int array ->
                decode_callstack_entry:
                  (location_code array -> int -> location list) ->
                obj_id
val put_collect : trace_writer -> timestamp -> obj_id -> unit
val put_promote : trace_writer -> timestamp -> obj_id -> unit
val put_event : trace_writer ->
                decode_callstack_entry:
                  (location_code array -> int -> location list) ->
                timestamp -> event -> unit
val flush : trace_writer -> unit

(** Reading traces *)
type trace_reader
val make_reader : Unix.file_descr -> trace_reader
val trace_info : trace_reader -> trace_info
val lookup_location : trace_reader -> location_code -> location list

type timedelta = int64

(** Iterate over a trace. Timestamps are in microseconds since trace start *)
val iter_trace : trace_reader -> ?parse_backtraces:bool -> (timedelta -> event -> unit) -> unit

(** For convenience, a hashtable keyed by object ID *)
module IdTbl : Hashtbl.SeededS with type key = obj_id

(** Convenience functions for accessing traces stored in files *)
val open_trace : filename:string -> trace_reader
val trace_size_bytes : trace_reader -> int64
val close_trace : trace_reader -> unit
