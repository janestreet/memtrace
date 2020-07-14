(** If the MEMTRACE environment variable is set, begin tracing to the file
    it specifies, and continue tracing until the process exits.

    The context is an arbitrary string, which is logged in the trace.
    It may be useful to identify trace files.

    The sampling_rate is the proportion of allocated words that should be
    sampled. Values larger than about 1e-4 will have some performance impact.
    The sampling rate can also be specified with the MEMTRACE_RATE environment
    variable. If both means are used, the env var takes precedence.

    May raise Unix.Unix_error if the specified file cannot be opened. *)
val trace_if_requested : ?context:string -> ?sampling_rate:float -> unit -> unit

(** Tracing can also be manually started and stopped. *)
type tracer

(** Manually start tracing *)
val start_tracing :
  context:string option ->
  sampling_rate:float ->
  filename:string ->
  tracer

(** Manually stop tracing *)
val stop_tracing : tracer -> unit

(** Use the Trace module to read and write trace files *)
module Trace = Trace

(** Use Memprof_tracer in conjunction with Trace.Writer for more manual
    control over trace collection *)
module Memprof_tracer = Memprof_tracer
