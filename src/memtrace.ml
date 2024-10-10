type tracer = Memprof_tracer.t

let getpid64 () = Int64.of_int (Unix.getpid ())

let start_tracing ~context ~sampling_rate ~filename =
  if Memprof_tracer.active_tracer () <> None then
    failwith "Only one Memtrace instance may be active at a time";
  let fd =
    try Unix.openfile filename Unix.[O_CREAT;O_WRONLY] 0o600
    with Unix.Unix_error (err, _, _) ->
      raise (Invalid_argument ("Cannot open memtrace file " ^ filename ^
                               ": " ^ Unix.error_message err))
  in
  begin
    try Unix.lockf fd F_TLOCK 0
    with Unix.Unix_error _ ->
      Unix.close fd;
      raise (Invalid_argument ("Cannot lock memtrace file " ^ filename ^
                               ": is another process using it?"))
  end;
  begin
    try Unix.ftruncate fd 0
    with Unix.Unix_error _ ->
      (* On special files (e.g. /dev/null), ftruncate fails. Ignoring errors
         here gives us the truncate-if-a-regular-file behaviour of O_TRUNC. *)
      ()
  end;
  let info : Trace.Info.t =
    { sample_rate = sampling_rate;
      word_size = Sys.word_size;
      executable_name = Sys.executable_name;
      host_name = Unix.gethostname ();
      ocaml_runtime_params = Sys.runtime_parameters ();
      pid = getpid64 ();
      start_time = Trace.Timestamp.now ();
      context;
    } in
  let trace = Trace.Writer.create fd ~getpid:getpid64 info in
  Memprof_tracer.start ~sampling_rate trace

let stop_tracing t =
  Memprof_tracer.stop t

let () =
  at_exit (fun () -> Option.iter stop_tracing (Memprof_tracer.active_tracer ()))

let default_sampling_rate = 1e-6

let trace_if_requested ?context ?sampling_rate () =
  match Sys.getenv_opt "MEMTRACE" with
  | None | Some "" -> ()
  | Some filename ->
     (* Prevent spawned OCaml programs from being traced *)
     Unix.putenv "MEMTRACE" "";
     let check_rate = function
       | Some rate when 0. < rate && rate <= 1. -> rate
       | _ ->
         raise (Invalid_argument ("Memtrace.trace_if_requested: " ^
                                  "sampling_rate must be between 0 and 1"))
     in
     let sampling_rate =
       match Sys.getenv_opt "MEMTRACE_RATE" with
       | Some rate -> check_rate (float_of_string_opt rate)
       | None ->
         match sampling_rate with
         | Some _ -> check_rate sampling_rate
         | None -> default_sampling_rate
     in
     let _s = start_tracing ~context ~sampling_rate ~filename in
     ()

module Trace = Trace
module Memprof_tracer = Memprof_tracer

module External = struct
  type token = Memprof_tracer.ext_token
  let alloc = Memprof_tracer.ext_alloc
  let free = Memprof_tracer.ext_free
end
module Geometric_sampler = Geometric_sampler
