@@ portable

open Stdlib_shim

val start
  :  ?report_exn:(exn -> unit) @ portable
  -> sampling_rate:float
  -> fd:Unix.file_descr
  -> ?getpid:(unit -> int64) @ portable
  -> ?write:(Unix.file_descr -> bytes @ local -> int -> int -> int) @ portable
  -> info:Trace.Info.t
  -> unit
  -> unit

val stop : unit -> unit
val active_tracer : unit -> bool
val current_domain : unit -> Trace.Domain_id.t

type ext_token : value mod contended external_ global immutable non_float

val ext_alloc : bytes:int -> ext_token or_null
val ext_free : ext_token -> unit (* can be called from async contexts *)
