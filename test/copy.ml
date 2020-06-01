open Memtrace.Trace

let copy inf outf =
  let r = open_trace ~filename:inf in
  let wfd = Unix.openfile outf [O_CREAT;O_WRONLY;O_TRUNC] 0o600 in
  let info = trace_info r in
  let w = make_writer wfd ~getpid:(fun () -> info.pid) info in
  iter_trace r (fun now ev ->
    put_event w
      ~decode_callstack_entry:(fun stk i ->
        lookup_location r stk.(i))
      (Int64.add now (trace_info r).start_time) ev);
  close_trace r;
  flush w;
  Unix.close wfd

let () =
  match Sys.argv with
  | [| _; inf; outf |] ->
     copy inf outf
  | _ -> Printf.fprintf stderr "usage: copy <in> <out>\n%!"; exit 1
