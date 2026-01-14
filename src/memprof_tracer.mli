open Stdlib_shim

type t

val start
  :  ?report_exn:(exn -> unit) @ portable
  -> sampling_rate:float
  -> Trace.Writer.t
  -> t

val stop : t -> unit
val active_tracer : unit -> t option
val current_domain : unit -> Trace.Domain_id.t

type ext_token : immediate

val ext_alloc : bytes:int -> ext_token or_null
val ext_free : ext_token -> unit
