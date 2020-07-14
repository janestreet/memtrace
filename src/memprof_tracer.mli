type t
val start : ?report_exn:(exn -> unit) -> sampling_rate:float -> Trace.Writer.t -> t
val stop : t -> unit
