let verbose = Sys.getenv_opt "VERBOSE" <> None

(* Check that a trace file is vaguely sensible. (The assertions internal to
   Memtrace.Trace.Reader are just as important as anything here) *)
let check_file filename =
  if verbose
  then Printf.printf "Checking %s (%5d bytes)... %!" filename (Unix.stat filename).st_size;
  if (Unix.stat filename).st_size = 0
  then () (* Possible if stopped even before it could write the header *)
  else (
    let r = Memtrace.Trace.Reader.open_ ~filename in
    let module Obj_tbl = Memtrace.Trace.Obj_id.Tbl in
    let seen_ids = Obj_tbl.create 10 in
    Memtrace.Trace.Reader.iter r (fun _ ev ->
      match ev with
      (* Check that Obj_ids are distinct and that each object has states: Alloc ->
         Promote? -> Collect *)
      | Alloc info ->
        assert (not (Obj_tbl.mem seen_ids info.obj_id));
        Obj_tbl.add seen_ids info.obj_id `Alloc
      | Promote (id, _) ->
        assert (Obj_tbl.mem seen_ids id);
        assert (Obj_tbl.find seen_ids id = `Alloc);
        Obj_tbl.replace seen_ids id `Promote
      | Collect (id, _) ->
        assert (Obj_tbl.mem seen_ids id);
        let state = Obj_tbl.find seen_ids id in
        assert (state = `Alloc || state = `Promote);
        Obj_tbl.replace seen_ids id `Collect);
    Memtrace.Trace.Reader.close r);
  if verbose then Printf.printf "done\n%!";
  ()
;;

(* Accept connections containing a memtrace on a socket *)
let handle_server srv pipe_done =
  let buf = Bytes.make 4096 '\000' in
  let fd_tbl = Hashtbl.create 10 in
  let handle_new fd =
    let filename = Filename.temp_file "trace." ".ctf" in
    let out_fd = Unix.openfile filename [ O_WRONLY ] 0 in
    Hashtbl.add fd_tbl fd (filename, out_fd)
  in
  let rec handle_read fd =
    let filename, out_fd = Hashtbl.find fd_tbl fd in
    match Unix.read fd buf 0 (Bytes.length buf) with
    | 0 ->
      Unix.close fd;
      Unix.close out_fd;
      check_file filename;
      Unix.unlink filename;
      Hashtbl.remove fd_tbl fd;
      None
    | n ->
      let m = Unix.write out_fd buf 0 n in
      assert (m = n);
      handle_read fd
    | exception Unix.Unix_error ((EAGAIN | EINTR | EWOULDBLOCK), _, _) -> Some fd
  in
  let rec loop fds is_done =
    let select_fds = srv :: fds in
    let select_fds = if is_done then select_fds else pipe_done :: select_fds in
    let _, _, _ = Unix.select select_fds [] select_fds (-1.) in
    let fds = List.filter_map handle_read fds in
    let is_done =
      if is_done
      then true
      else (
        match Unix.read pipe_done (Bytes.make 1 '\000') 0 1 with
        | n ->
          assert (n = 0);
          Unix.close pipe_done;
          true
        | exception Unix.Unix_error ((EAGAIN | EINTR | EWOULDBLOCK), _, _) -> false)
    in
    match Unix.accept srv with
    | fd, _addr ->
      Unix.set_nonblock fd;
      handle_new fd;
      loop (fd :: fds) is_done
    | exception Unix.Unix_error ((EAGAIN | EINTR | EWOULDBLOCK), _, _) ->
      if fds = [] && is_done then () else loop fds is_done
  in
  loop [] false
;;

(* Fork a child process that listens on an anonymous Unix socket, accepting memtrace files *)
let server_addr, server_pid, pipe_done =
  let open Unix in
  let sock = socket PF_UNIX SOCK_STREAM 0 in
  set_nonblock sock;
  (* Bind to an empty socket name so the OS chooses one *)
  bind sock (ADDR_UNIX "");
  listen sock 10;
  let pipe_rd, pipe_wr = pipe () in
  set_nonblock pipe_rd;
  match Unix.fork () with
  | 0 ->
    (* child *)
    Unix.close pipe_wr;
    handle_server sock pipe_rd;
    Stdlib.exit 0
  | pid ->
    (* parent *)
    Unix.close pipe_rd;
    let addr = getsockname sock in
    Unix.close sock;
    addr, pid, pipe_wr
;;

let new_fd () =
  let open Unix in
  let sock = socket PF_UNIX SOCK_STREAM 0 in
  connect sock server_addr;
  sock
;;

let start_memtrace () =
  let fd = new_fd () in
  let info : Memtrace.Trace.Info.t =
    { sample_rate = 0.1
    ; word_size = 64
    ; executable_name = "exec"
    ; host_name = "host"
    ; ocaml_runtime_params = "runtime"
    ; pid = 42L
    ; initial_domain = Memtrace.Memprof_tracer.current_domain ()
    ; start_time = Memtrace.Trace.Timestamp.of_int64 23897423L
    ; context = Some "context"
    }
  in
  Memtrace.Memprof_tracer.start ~sampling_rate:info.sample_rate ~fd ~info ()
;;

let stop_memtrace () = Memtrace.Memprof_tracer.stop ()

(* domains to create, not counting main *)
let avail_domains = Atomic.make (Domain.recommended_domain_count () - 1)

(* threads to create, not counting domain main threads *)
let avail_threads = Atomic.make 20

let rec do_stuff ~extra count =
  if count <= 0
  then ()
  else (
    let count = count - 1 in
    let choice = Random.int 1000 in
    match choice with
    | 0 ->
      start_memtrace ();
      do_stuff ~extra count
    | 1 ->
      stop_memtrace ();
      do_stuff ~extra count
    | (2 | 3) when count > 100 ->
      let use_domain = choice = 2 in
      let avail_counter = if use_domain then avail_domains else avail_threads in
      let avail = Atomic.get avail_counter in
      if avail > 0 && Atomic.compare_and_set avail_counter avail (avail - 1)
      then (
        let tail_budget = Random.int count in
        let child_budget = Random.int (count - tail_budget) in
        let child_action () =
          try do_stuff ~extra:None child_budget with
          | e ->
            print_string ("Exception: " ^ Printexc.to_string e ^ "\n");
            Printexc.print_backtrace stdout;
            raise e
        in
        let join_fn =
          if use_domain
          then (
            let d = (Domain.Safe.spawn [@alert "-do_not_spawn_domains"]) child_action in
            fun () -> Domain.join d)
          else (
            let th = Thread.Portable.create child_action () in
            fun () -> Thread.join th)
        in
        do_stuff ~extra (count - tail_budget - child_budget);
        join_fn ();
        Atomic.incr avail_counter;
        do_stuff ~extra tail_budget)
      else do_stuff ~extra count
    | 4 ->
      Gc.minor ();
      do_stuff ~extra count
    | 5 ->
      Gc.major ();
      do_stuff ~extra count
    | n when n < 10 && extra <> None ->
      (Option.get extra) ();
      do_stuff ~extra count
    | _ ->
      if Random.bool ()
      then (
        let _ : string array @ global =
          Array.make (Random.int 500) "hello" |> Sys.opaque_identity
        in
        ())
      else (
        match Memtrace.External.alloc ~bytes:(Random.int 50_000) with
        | Null -> ()
        | This t -> Memtrace.External.free t);
      do_stuff ~extra count)
;;

let () =
  Random.self_init ();
  (* Lower obj_ids_per_chunk to give Obj_id.Allocator a workout *)
  Atomic.set Memtrace.Trace.Private.obj_ids_per_chunk 100;
  (* Additional non-portable operations, done only on main domain/thread *)
  let extra () =
    match Memtrace.External.alloc ~bytes:(Random.int 50_000) with
    | Null -> ()
    | This t -> Gc.finalise (fun t -> Memtrace.External.free !t) (ref t)
  in
  do_stuff ~extra:(Some extra) 5_000_000;
  stop_memtrace ();
  Unix.close pipe_done;
  match Unix.waitpid [] server_pid with
  | _, WEXITED 0 -> ()
  | _ -> assert false
;;
