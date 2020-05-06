(*
module Deps = struct
  type out_file = Unix.file_descr
  let open_out filename =
    Unix.openfile filename Unix.[O_CREAT;O_WRONLY;O_TRUNC] 0o666
  let close_out fd = Unix.close fd

  type in_file = Unix.file_descr
  let open_in filename =
    Unix.openfile filename [Unix.O_RDONLY] 0
  let close_in fd = Unix.close fd

  let timestamp () = Unix.gettimeofday ()

  type allocation (*= Gc.Memprof.allocation*) = private
    { n_samples : int;
      size : int;
      unmarshalled : bool;
      callstack : Printexc.raw_backtrace
    }

  let memprof_start
    ~callstack_size
    ~(minor_alloc_callback:allocation -> _)
    ~(major_alloc_callback:allocation -> _)
    ~promote_callback
    ~minor_dealloc_callback
    ~major_dealloc_callback
    ~sampling_rate
    () : unit =
(*
    ignore (callstack_size, minor_alloc_callback, major_alloc_callback,
            promote_callback, minor_dealloc_callback, major_dealloc_callback, sampling_rate);
    assert false
*)
    (*Gc.Memprof.start ~callstack_size ~minor_alloc_callback ~major_alloc_callback
      ~promote_callback ~minor_dealloc_callback ~major_dealloc_callback ~sampling_rate*)
      ()
  let memprof_stop () : unit = (*Gc.Memprof.stop*) ()

end

(* Increment this when the format changes *)

module IntTbl = Hashtbl.MakeSeeded (struct
  type t = int
  let hash _seed (id : t) =
    let h = id * 189696287 in
    h lxor (h lsr 23)
  let equal (a : t) (b : t) = a = b
end)

open Buf

open Trace



let[@inline never] lock_tracer s =
  (* This is a maximally unfair spinlock. *)
  (* FIXME: correctness rests on dubious assumptions of atomicity *)
  (* if s.locked then Printf.fprintf stderr "contention\n%!"; *)
  while s.locked do Thread.yield () done;
  s.locked <- true

let[@inline never] unlock_tracer s =
  assert (s.locked);
  s.locked <- false






let start_tracing ~sampling_rate ~filename =
  let dest = Deps.open_out filename in
  let now = Deps.timestamp () in
  (* FIXME magic number sizes *)
{
    stopped = false;
    locked = false
  } in
  Deps.memprof_start
    ~callstack_size:max_int
    ~minor_alloc_callback:(fun info ->
      lock_tracer s;
      let r = log_alloc s false info.callstack info.size info.n_samples in
      unlock_tracer s; r)
    ~major_alloc_callback:(fun info ->
      lock_tracer s;
      let r = log_alloc s true info.callstack info.size info.n_samples in
      unlock_tracer s; r)
    ~promote_callback:(fun id ->
      lock_tracer s;
      let r = log_promote s id in
      unlock_tracer s; r)
    ~minor_dealloc_callback:(fun id ->
      lock_tracer s;
      let r = log_collect s id in
      unlock_tracer s; r)
    ~major_dealloc_callback:(fun id ->
      lock_tracer s;
      let r = log_collect s id in
      unlock_tracer s; r)
    ~sampling_rate
    ();
  s

let stop_tracing s =
  if not s.stopped then begin
    s.stopped <- true;
    Deps.memprof_stop ();
    flush s;
    Deps.close_out s.dest
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

type timestamp = Int64.t
type location_code = int

type trace = {
  fd : Deps.in_file;
  info : trace_info;
  data_off : int;
  (* FIXME: opt to better hashtable *)
  loc_table : location list IntTbl.t
}

let trace_info { info; _ } = info

let lookup_location { loc_table; _ } code =
  match IntTbl.find loc_table code with
  | v -> v
  | exception Not_found ->
    raise (Invalid_argument "invalid location code")
(*    [{ filename = "<bad>"; line = 0; start_char = 0; end_char = 0; defname = "<bad>" }]*)


let iter_trace {fd; loc_table; data_off; info = { start_time; _ } } ?(parse_backtraces=true) f =
  let cache = create_reader_cache () in
  let file_mtf = create_mtf_table () in
  let defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ()) in
  Unix.lseek fd data_off SEEK_SET |> ignore;
  (* FIXME error handling *)
  let buf = Bytes.make (1 lsl 18) '\000' in

let open_trace ~filename =
  let fd = Deps.open_in filename in
  let file_size = (Unix.LargeFile.fstat fd).st_size in

  (* FIXME magic numbers *)
  let buf = Bytes.make (1 lsl 15) '\000' in
  let b = Buf.read_fd fd buf in
  let packet_info = get_ctf_header b in
  check_fmt "trace info packet" (remaining b >= packet_info.content_size);
  let ev, evtime = get_event_header packet_info b in
  check_fmt "trace info packet" (ev = Ev_trace_info);
  check_fmt "trace info packet" (evtime = packet_info.time_begin);
  let trace_info = get_trace_info b packet_info.time_begin file_size in
  let loc_table = IntTbl.create 20 in
  { fd; info = trace_info; data_off = b.pos; loc_table }

let close_trace t =
  Deps.close_in t.fd

module IdTbl = IntTbl
 *)

module Trace = Trace
include Trace                   (* FIXME *)
