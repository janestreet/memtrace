val trace_until_exit : sampling_rate:float -> filename:string -> unit

type tracer
val start_tracing : sampling_rate:float -> filename:string -> tracer
val stop_tracing : tracer -> unit


type trace
val open_trace : filename:string -> trace

type timestamp = Int64.t
val to_unix_timestamp : timestamp -> float

type timedelta = Int64.t  (* microseconds *)

type trace_info = {
  sample_rate : float;
  executable_name : string;
  host_name : string;
  start_time : timestamp;
  pid : Int32.t
}
val trace_info : trace -> trace_info

type obj_id = private int
type location_code = private Int64.t

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
    obj_id : obj_id;
    length : int;
    nsamples : int;
    is_major : bool;
    (* NB: this is a mutable buffer, reused between events.
       If you want to store backtraces, you must copy them
       from this buffer using:
       [Array.sub backtrace_buffer 0 backtrace_length] *)
    backtrace_buffer : location_code array;
    backtrace_length : int;
    (* A prefix of this length has not changed since the last event *)
    common_prefix : int;
  }
  | Promote of obj_id
  | Collect of obj_id

val iter_trace : trace -> ?parse_backtraces:bool -> (timedelta -> event -> unit) -> unit

val close_trace : trace -> unit
