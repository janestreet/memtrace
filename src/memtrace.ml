type tracer =
  { mutable locked : bool;
    mutable stopped : bool;
    fd : Unix.file_descr;
    trace : Trace.trace_writer }

let[@inline never] lock_tracer s =
  (* This is a maximally unfair spinlock. *)
  (* if s.locked then Printf.fprintf stderr "contention\n%!"; *)
  while s.locked do Thread.yield () done;
  s.locked <- true

let[@inline never] unlock_tracer s =
  assert (s.locked);
  s.locked <- false


let decode_entry callstack pos : Trace.location list = 
  let open Printexc in
  let rec get_locations slot : Trace.location list =
    let tail =
      match get_raw_backtrace_next_slot slot with
      | None -> []
      | Some slot -> get_locations slot in
    let slot = convert_raw_backtrace_slot slot in
    match Slot.location slot with
    | None -> tail
    | Some { filename; line_number; start_char; end_char } ->
       let defname = match Slot.name slot with Some name -> name | _ -> "??" in
       { filename; line=line_number; start_char; end_char; defname } :: tail in
  get_locations (get_raw_backtrace_slot callstack pos) |> List.rev

let getpid64 () = Int64.of_int (Unix.getpid ())

let now () = Trace.timestamp_of_float (Unix.gettimeofday ())

let start_tracing ~sampling_rate ~filename =
  let fd = Unix.openfile filename Unix.[O_CREAT;O_WRONLY;O_TRUNC] 0o600 in
  let info : Trace.trace_info =
    { sample_rate = sampling_rate;
      word_size = Sys.word_size;
      executable_name = Sys.executable_name;
      host_name = Unix.gethostname ();
      ocaml_runtime_params = Sys.runtime_parameters ();
      pid = getpid64 ();
      start_time = now ()
    } in
  let trace = Trace.make_writer fd ~getpid:getpid64 info in
  let s = { fd; trace; locked = false; stopped = false } in
  (* Unfortunately, efficient access to the backtrace is not possible
     with the current Printexc API, even though internally it's an int
     array. For now, wave the Obj.magic wand. *)
  Gc.Memprof.start
    ~callstack_size:max_int
    ~minor_alloc_callback:(fun info ->
      lock_tracer s;
      let r = Trace.put_alloc trace (now ())
                ~length:info.size
                ~nsamples:info.n_samples
                ~is_major:false
                ~callstack:(Obj.magic info.callstack)
                ~decode_callstack_entry:(fun stk pos -> decode_entry (Obj.magic stk) pos) in
      unlock_tracer s; Some r)
    ~major_alloc_callback:(fun info ->
      lock_tracer s;
      let r = Trace.put_alloc trace (now ())
                ~length:info.size
                ~nsamples:info.n_samples
                ~is_major:true
                ~callstack:(Obj.magic info.callstack)
                ~decode_callstack_entry:(fun stk pos -> decode_entry (Obj.magic stk) pos) in
      unlock_tracer s; Some r)
    ~promote_callback:(fun id ->
      lock_tracer s;
      Trace.put_promote trace (now ()) id;
      unlock_tracer s; Some id)
    ~minor_dealloc_callback:(fun id ->
      lock_tracer s;
      Trace.put_collect trace (now ()) id;
      unlock_tracer s)
    ~major_dealloc_callback:(fun id ->
      lock_tracer s;
      Trace.put_collect trace (now ()) id;
      unlock_tracer s)
    ~sampling_rate
    ();
  s

let stop_tracing s =
  if not s.stopped then begin
    s.stopped <- true;
    Gc.Memprof.stop ();
    Trace.flush s.trace;
    Unix.close s.fd;
  end

let trace_until_exit ~sampling_rate ~filename =
  let s = start_tracing ~sampling_rate ~filename in
  at_exit (fun () -> stop_tracing s)

let trace_if_requested ?(sampling_rate=0.0001) () =
  match Sys.getenv_opt "MEMTRACE" with
  | None | Some "" -> ()
  | Some filename ->
     (* Prevent spawned OCaml programs from being traced *)
     Unix.putenv "MEMTRACE" "";
     trace_until_exit ~sampling_rate ~filename

module Trace = Trace
