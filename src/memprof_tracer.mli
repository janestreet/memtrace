type t
val start : ?record_gc_events:bool -> ?report_exn:(exn -> unit) -> sampling_rate:float -> Trace.Writer.t -> t
val stop : t -> unit

val active_tracer : unit -> t option


type ext_token [@@immediate]
val ext_alloc : bytes:int -> ext_token option
val ext_free : ext_token -> unit
