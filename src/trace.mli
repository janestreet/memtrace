type timestamp = int64
val float_of_timestamp : timestamp -> float
val timestamp_of_float : float -> timestamp

type trace_info = {
  sample_rate : float;
  word_size : int;
  executable_name : string;
  host_name : string;
  ocaml_runtime_params : string;
  pid : Int64.t;
  start_time : timestamp;
}

type location = {
  filename : string;
  line : int;
  start_char : int;
  end_char : int;
  defname : string;
}
val string_of_location : location -> string

type obj_id = private int
type location_code = int
type event =
  | Alloc of {
    obj_id : obj_id;
    length : int;
    nsamples : int;
    is_major : bool;
    backtrace_buffer : location_code array;
    backtrace_length : int;
    common_prefix : int;
  }
  | Promote of obj_id
  | Collect of obj_id
val string_of_event : (location_code -> location list) -> event -> string


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

type trace_reader
val make_reader : Unix.file_descr -> trace_reader
val trace_info : trace_reader -> trace_info
val lookup_location : trace_reader -> location_code -> location list
val iter_trace : trace_reader -> ?parse_backtraces:bool -> (timestamp -> event -> unit) -> unit

module IdTbl : Hashtbl.SeededS with type key = obj_id

val open_trace : filename:string -> trace_reader
val trace_size_bytes : trace_reader -> int64
val close_trace : trace_reader -> unit
