open Memtrace.Trace

let copy inf outf fact tstart tend =
  let id_remap = Obj_id.Tbl.create 20 in
  let r = Reader.open_ ~filename:inf in
  let wfd = Unix.openfile outf [ O_CREAT; O_WRONLY; O_TRUNC ] 0o600 in
  let info = Reader.info r in
  let info = { info with sample_rate = info.sample_rate /. float_of_int fact } in
  let pid = info.pid in
  let w = Writer.Multiplexed_domains.create wfd ~getpid:(fun () -> pid) info in
  Reader.iter r (fun now ev ->
    let ev : Event.t option =
      match ev with
      | Alloc ({ obj_id; _ } as e) ->
        let now_i = Int64.to_int (Int64.div (Timedelta.to_int64 now) 1_000_000L) in
        if tstart <= now_i && now_i < tend
        then (
          let samples = ref 0 in
          for _i = 1 to e.nsamples do
            if Random.int fact = 0 then incr samples
          done;
          if !samples > 0
          then (
            let id = Writer.Multiplexed_domains.next_alloc_id w ~domain:e.domain in
            let ev = Event.Alloc { e with obj_id = id; nsamples = !samples } in
            Obj_id.Tbl.add id_remap obj_id id;
            Some ev)
          else None)
        else None
      | Promote (id, dom) ->
        if Obj_id.Tbl.mem id_remap id
        then Some (Promote (Obj_id.Tbl.find id_remap id, dom))
        else None
      | Collect (id, dom) ->
        if Obj_id.Tbl.mem id_remap id
        then (
          let id' = Obj_id.Tbl.find id_remap id in
          Obj_id.Tbl.remove id_remap id;
          Some (Event.Collect (id', dom)))
        else None
    in
    match ev with
    | None -> ()
    | Some ev ->
      Writer.Multiplexed_domains.put_event
        w
        ~decode_callstack_entry:(fun loc -> Reader.lookup_location_code r loc)
        (Timedelta.offset info.start_time now)
        ev);
  Reader.close r;
  Writer.Multiplexed_domains.flush w;
  Unix.close wfd
;;

let parseint f =
  match int_of_string f with
  | n -> n
  | exception _ ->
    Printf.fprintf stderr "argument must be an integer (got %s)\n" f;
    exit 1
;;

let () =
  match Sys.argv with
  | [| _; inf; outf; fact |] ->
    let fact = parseint fact in
    copy inf outf fact min_int max_int
  | [| _; inf; outf; fact; tstart; tend |] ->
    let fact = parseint fact in
    let tstart = parseint tstart in
    let tend = parseint tend in
    copy inf outf fact tstart tend
  | _ ->
    Printf.fprintf
      stderr
      "usage: subsample <in> <out> <subsampling factor> [<start time> <end time>]\n%!";
    exit 1
;;
