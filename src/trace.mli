(** Encoder and decoder for Memtrace traces *)

(* CR-someday dkalinichenko: reorganise the imports here. *)
module Timestamp = Gc_recent_events.Timestamp

(** Times measured from the start of the trace *)
module Timedelta : sig
  type t

  (** Convert to the number of microseconds since the start of the trace *)
  val to_int64 : t -> int64
  val offset : Timestamp.t -> t -> Timestamp.t
  val delta : Timestamp.t -> Timestamp.t -> t
end

(** Source locations in the traced program *)
module Location : sig
  type t = {
    filename : string;
    line : int;
    start_char : int;
    end_char : int;
    defname : string;
  }

  val to_string : t -> string
  val unknown : t
end

(** Identifiers to represent allocations *)
module Obj_id : sig
  type t = private int

  (** For convenience, a hashtable keyed by object ID *)
  module Tbl : Hashtbl.SeededS with type key = t
end

(** Codes for subsequences of locations in a backtrace *)
module Location_code : sig
  type t = private int

  (** For convenience, a hashtable keyed by location code *)
  module Tbl : Hashtbl.SeededS with type key = t
end

(** Types of allocation *)
module Allocation_source : sig
  type t = Minor | Major | External
end

(* Timedelta representation of GC events *)
module Gc_event : sig
  type t = {
    data : Gc_recent_events.Event_data.t;
    begin_ : Timedelta.t;
    end_ : Timedelta.t
  }

  val to_string : t -> string
end

(** Trace events *)
module Event : sig
  type t =
    | Alloc of {
        obj_id : Obj_id.t;
        (** An identifier for this allocation, used to refer to it in other events.
            These identifiers are generated in allocation order. *)
        length : int;
        (** Length of the sampled allocation, in words, not including header word *)
        nsamples : int;
        (** Number of samples made in this allocation. At least 1. *)
        source : Allocation_source.t;
        (** How this object was initially allocated *)
        backtrace_buffer : Location_code.t array;
        (** Backtrace of the allocation.
            The backtrace elements are stored in order from caller to callee.
            The first element is the main entrypoint and the last is the allocation.

            NB: this is a mutable buffer, reused between events.
            Entries at indices beyond [backtrace_length - 1] are not meaningful.
            If you want to store backtraces, you must copy them using:
            [Array.sub backtrace_buffer 0 backtrace_length]. *)
        backtrace_length : int;
        (** Length of the backtrace *)
        common_prefix : int;
        (** A prefix of this length has not changed since the last event *)
      }
    | Promote of Obj_id.t
    | Collect of Obj_id.t
    | Gc_event of Gc_event.t

  val to_string : (Location_code.t -> Location.t list) -> t -> string
end

(** Global trace info *)
module Info : sig
  type t = {
    sample_rate : float;
    word_size : int;
    executable_name : string;
    host_name : string;
    ocaml_runtime_params : string;
    pid : Int64.t;
    start_time : Timestamp.t;
    context : string option;
  }
end

(** Writing traces *)
module Writer : sig
  type t
  exception Pid_changed
  val create : Unix.file_descr -> ?getpid:(unit -> int64) -> Info.t -> t

  (** All of the functions below may raise Unix_error if
      writing to the file descriptor fails, or Pid_changed
      if getpid returns a different value. *)

  val put_alloc :
    t
    -> Timestamp.t
    -> length:int
    -> nsamples:int
    -> source:Allocation_source.t
    -> callstack:Location_code.t array
    -> decode_callstack_entry:(Location_code.t -> Location.t list)
    -> Obj_id.t
  val put_alloc_with_raw_backtrace :
    t
    -> Timestamp.t
    -> length:int
    -> nsamples:int
    -> source:Allocation_source.t
    -> callstack:Printexc.raw_backtrace
    -> Obj_id.t
  val put_collect : t -> Timestamp.t -> Obj_id.t -> unit
  val put_promote : t -> Timestamp.t -> Obj_id.t -> unit
  val put_gc_event : t -> Timestamp.t -> Gc_event.t -> unit
  val put_timestamped_gc_event : t -> Timestamp.t -> Gc_recent_events.Event.t -> unit
  val put_event :
    t
    -> decode_callstack_entry:(Location_code.t -> Location.t list)
    -> Timestamp.t
    -> Event.t
    -> unit

  val flush : t -> unit
  val close : t -> unit
end

(** Reading traces *)
module Reader : sig
  type t

  val create : Unix.file_descr -> t
  val info : t -> Info.t
  val lookup_location_code : t -> Location_code.t -> Location.t list

  (** Iterate over a trace *)
  val iter : t -> ?parse_backtraces:bool -> (Timedelta.t -> Event.t -> unit) -> unit

  (** Convenience functions for accessing traces stored in files *)
  val open_ : filename:string -> t
  val size_bytes : t -> int64
  val close : t -> unit
end
