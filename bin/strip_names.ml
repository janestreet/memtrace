module Anon_trace = struct
  type obj_id = Memtrace.Trace.Obj_id.t
  type dom_id = Memtrace.Trace.Domain_id.t
  type timestamp = Memtrace.Trace.Timestamp.t
  type timedelta = Memtrace.Trace.Timedelta.t

  type allocation_source = Memtrace.Trace.Allocation_source.t =
    | Minor
    | Major
    | External

  type event =
    | Alloc of
        { obj_id : obj_id
        ; domain : dom_id
        ; length : int
        ; nsamples : int
        ; source : allocation_source
        }
    | Promote of obj_id * dom_id
    | Collect of obj_id * dom_id

  type trace_info =
    { sample_rate : float
    ; word_size : int
    ; ocaml_runtime_params : string
    }
end

module Anon_writer : sig
  type t

  val create : Unix.file_descr -> Anon_trace.trace_info -> t
  val put_event : t -> Anon_trace.timedelta -> Anon_trace.event -> unit
  val flush : t -> unit
end = struct
  open Anon_trace
  module Trace = Memtrace.Trace
  module Writer = Trace.Writer.Multiplexed_domains

  type t = Writer.t

  let start_time = Trace.Timestamp.of_int64 0L

  let create fd info =
    let { sample_rate; word_size; ocaml_runtime_params } = info in
    let info : Trace.Info.t =
      { sample_rate
      ; word_size
      ; ocaml_runtime_params
      ; executable_name = "<anonymous>"
      ; host_name = "<anonymous>"
      ; pid = 0L
      ; initial_domain = Trace.Domain_id.main_domain
      ; start_time
      ; context = None
      }
    in
    Writer.create fd ~getpid:(fun () -> 0L) info
  ;;

  let put_event w time ev =
    let ev : Trace.Event.t =
      match ev with
      | Alloc { obj_id; length; domain; nsamples; source } ->
        Alloc
          { obj_id
          ; length
          ; domain
          ; nsamples
          ; source
          ; backtrace_buffer = [||]
          ; backtrace_length = 0
          ; common_prefix = 0
          }
      | Promote (id, dom) -> Promote (id, dom)
      | Collect (id, dom) -> Collect (id, dom)
    in
    Writer.put_event
      w
      (Trace.Timedelta.offset start_time time)
      ev
      ~decode_callstack_entry:(fun _ -> assert false)
  ;;

  let flush = Writer.flush
end

module Anon_reader : sig
  type t

  val open_ : filename:string -> t
  val info : t -> Anon_trace.trace_info
  val iter : t -> (Anon_trace.timedelta -> Anon_trace.event -> unit) -> unit
  val close : t -> unit
end = struct
  open Anon_trace
  module Trace = Memtrace.Trace
  module Reader = Trace.Reader

  type t = Reader.t

  let open_ = Reader.open_

  let info r =
    let { sample_rate; word_size; ocaml_runtime_params; _ } : Trace.Info.t =
      Reader.info r
    in
    { sample_rate; word_size; ocaml_runtime_params }
  ;;

  let iter r f =
    Reader.iter r (fun time (ev : Trace.Event.t) ->
      let ev =
        match ev with
        | Alloc { obj_id; length; domain; nsamples; source; _ } ->
          Alloc { obj_id; length; domain; nsamples; source }
        | Promote (id, dom) -> Promote (id, dom)
        | Collect (id, dom) -> Collect (id, dom)
      in
      f time ev)
  ;;

  let close = Reader.close
end

let strip_names infile outfile =
  let r = Anon_reader.open_ ~filename:infile in
  let wfd = Unix.openfile outfile [ O_CREAT; O_WRONLY; O_TRUNC ] 0o600 in
  let w = Anon_writer.create wfd (Anon_reader.info r) in
  Anon_reader.iter r (Anon_writer.put_event w);
  Anon_writer.flush w;
  Unix.close wfd;
  Anon_reader.close r
;;

let () =
  if Array.length Sys.argv <> 3
  then Printf.fprintf stderr "Usage: %s <input> <output>\n" Sys.executable_name
  else strip_names Sys.argv.(1) Sys.argv.(2)
;;
