(** If the MEMTRACE environment variable is set,
    begin tracing to the file it specifies *)
val trace_if_requested : ?sampling_rate:float -> unit -> unit

(** Unconditionally begin tracing to the specified file *)
val trace_until_exit : sampling_rate:float -> filename:string -> unit


(** Tracing can also be manually started and stopped. *)
type tracer

(** Manually start tracing *)
val start_tracing : sampling_rate:float -> filename:string -> tracer

(** Manually stop tracing *)
val stop_tracing : tracer -> unit


(** Use the Trace module to read and write trace files *)
module Trace = Trace
