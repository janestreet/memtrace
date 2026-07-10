@@ portable

open Stdlib_shim

(** If the MEMTRACE environment variable is set, begin tracing to the file it specifies,
    and continue tracing until the process exits.

    The context is an arbitrary string, which is logged in the trace. It may be useful to
    identify trace files.

    The sampling_rate is the proportion of allocated words that should be sampled. Values
    larger than about 1e-4 will have some performance impact. The sampling rate can also
    be specified with the MEMTRACE_RATE environment variable. If both means are used, the
    env var takes precedence.

    May raise Unix.Unix_error if the specified file cannot be opened, or Invalid_argument
    if the MEMTRACE_RATE parameter is ill-formed.

    May call failwith if memtrace was already started and [if_started] is `Fail. *)
val trace_if_requested
  :  ?if_started:[ `Fail | `Ignore ]
  -> ?context:string
  -> ?sampling_rate:float
  -> unit
  -> unit
  @@ nonportable

(** Manually start tracing. *)
val start_tracing
  :  context:string option
  -> sampling_rate:float
  -> filename:string
  -> unit

(** Manually stop tracing.

    This function does nothing if tracing is not in progress. *)
val stop_tracing : unit -> unit

(** Is there an active trace running at the moment? *)
val currently_tracing : unit -> bool

val default_sampling_rate : float

(** Use the Trace module to read and write trace files *)
module Trace = Trace

(** Use Memprof_tracer in conjunction with Trace.Writer for more manual control over trace
    collection *)
module Memprof_tracer = Memprof_tracer

(** Use External to track non-GC-heap allocations in a Memtrace trace *)
module External : sig
  (** tokens are immediate, but not portable *)
  type token : value mod contended external_ global immutable non_float

  (** [alloc ~bytes] reports an allocation of a given number of bytes.

      If tracing is enabled, a small fraction of the calls to this function will return
      [This tok], where [tok] should be passed to [free] when the object is freed.

      This function is very fast in the common case where it returns [Null] *)
  val alloc : bytes:int -> token or_null

  (** This function can be safely called from finalizers and signal handlers *)
  val free : token -> unit
end

(** Memtrace has a global singleton tracer, and this type is defined purely for
    compatibility *)
type tracer = unit

(** (For testing) *)
module Geometric_sampler = Geometric_sampler
