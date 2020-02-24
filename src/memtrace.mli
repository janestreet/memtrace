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
}

val lookup_location : trace -> location_code -> location list

type event =
  | Alloc of {
    obj_id : obj_id;
    length : int;
    nsamples : int;
    is_major : bool;
    common_prefix : int;
    new_suffix : location_code list;
  }
  | Promote of obj_id
  | Collect of obj_id

val iter_trace : trace -> (timedelta -> event -> unit) -> unit

val close_trace : trace -> unit
