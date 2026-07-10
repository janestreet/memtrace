open Stdlib_shim

val start
  :  ?report_exn:(exn -> unit)
  -> sampling_rate:float
  -> fd:Unix.file_descr
  -> ?getpid:(unit -> int64)
  -> ?write:(Unix.file_descr -> bytes -> int -> int -> int)
  -> info:Trace.Info.t
  -> unit
  -> unit

val stop : unit -> unit
val active_tracer : unit -> bool
val current_domain : unit -> Trace.Domain_id.t

type ext_token

val ext_alloc : bytes:int -> ext_token or_null
val ext_free : ext_token -> unit (* can be called from async contexts *)
