let check_errors () =
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE" "/bad/file/name";
  (match Memtrace.trace_if_requested () with
   | _ -> assert false
   | exception Invalid_argument _ -> ());
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE" "/tmp/goodfilename";
  (match Memtrace.trace_if_requested ~sampling_rate:(-3.) () with
   | _ -> assert false
   | exception Invalid_argument _ -> ());
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE" "/tmp/goodfilename";
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE_RATE" "42";
  (match Memtrace.trace_if_requested () with
   | _ -> assert false
   | exception Invalid_argument _ -> ());
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE" "/tmp/goodfilename";
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE_RATE" "potato";
  match Memtrace.trace_if_requested () with
  | _ -> assert false
  | exception Invalid_argument _ -> ()
;;

let () = check_errors ()

let is_bad_location (loc : Memtrace.Trace.Location.t) =
  let defname = loc.defname in
  String.ends_with defname ~suffix:"ext_alloc"
  || String.ends_with defname ~suffix:"ext_alloc_slowpath"
;;

let validate_ext_alloc_backtrace r bt len =
  (* Make sure the backtrace has been scrubbed of calls to [ext_alloc] and
     [ext_alloc_slowpath] in [memprof_tracer.ml] *)
  for i = 0 to len - 1 do
    let locs = Memtrace.Trace.Reader.lookup_location_code r bt.(i) in
    List.iter
      (fun (loc : Memtrace.Trace.Location.t) -> assert (not (is_bad_location loc)))
      locs
  done
;;

let globs = Array.make 1000 [||]
let nglobs = ref 0

let leak x =
  globs.(!nglobs) <- x;
  incr nglobs
;;

let each_iter () = leak (Array.make 1000 0)

let rec long_bt = function
  | 0 ->
    leak (Array.make 1000 0);
    (Sys.opaque_identity List.iter) each_iter [ () ];
    42
  | n -> if Random.bool () then 1 + long_bt (n - 1) else 2 + long_bt (n - 1)
;;

let leaked_ext_token = ref Null

let go () =
  (* Set ids_per_chunk low so that the Obj_id chunk allocation code actually gets tested *)
  Atomic.set Memtrace.Trace.Private.obj_ids_per_chunk 10;
  let filename = Filename.temp_file "memtrace" "ctf" in
  (* Check that we can recover from a bad sampling rate *)
  (match Memtrace.start_tracing ~context:(Some "ctx") ~sampling_rate:42.0 ~filename with
   | _ -> failwith "should have failed"
   | exception _ -> ());
  let dom_done = Atomic.make false in
  let dom_started = Atomic.make false in
  let dom_func () =
    Atomic.set dom_started true;
    let external_alloc () =
      match Memtrace.External.alloc ~bytes:(Sys.word_size / 8 * 7) with
      | Null -> ()
      | This token -> Memtrace.External.free token
    in
    if Random.bool () then external_alloc ();
    (* 50% of the time, start with an ext alloc *)
    for _i = 1 to 1000 do
      let _ : int array = Array.make 397 42 |> Sys.opaque_identity in
      external_alloc ()
    done;
    Atomic.set dom_done true
  in
  let _ : _ = Sys.opaque_identity dom_func in
  let t = Memtrace.start_tracing ~context:(Some "ctx") ~sampling_rate:0.1 ~filename in
  (* Test a Memtrace.External token leaking from one trace to another *)
  (match !leaked_ext_token with
   | This tok -> Memtrace.External.free tok
   | Null -> ());
  let rec leak_ext () =
    match Memtrace.External.alloc ~bytes:(256 * (Sys.word_size / 8)) with
    | Null -> leak_ext ()
    | This tok -> tok
  in
  leaked_ext_token := This (leak_ext ());
  let spawn_traced_domain () =
    Some ((Domain.Safe.spawn [@alert "-do_not_spawn_domains"]) dom_func)
  in
  let domain =
    if Domain.recommended_domain_count () <= 1
    then (
      Atomic.set dom_done true;
      None)
    else spawn_traced_domain ()
  in
  leak (Array.make 4242 42);
  for _i = 1 to 10 do
    let n = long_bt 10_000 in
    assert (n > 0)
  done;
  for _i = 1 to 1000 do
    match Memtrace.External.alloc ~bytes:(Sys.word_size / 8 * 7) with
    | Null -> ()
    | This token -> Memtrace.External.free token
  done;
  while not (Atomic.get dom_done) do
    Sys.poll_actions ()
  done;
  (* Test domain termination logic by giving the other domain some time to finish, and
     doing a full GC cycle after it has orphaned any outstanding memprof state *)
  Unix.sleepf 0.1;
  Gc.full_major ();
  Memtrace.stop_tracing t;
  Option.iter Domain.join domain;
  (* after stop_tracing because it allocates *)
  let r = Memtrace.Trace.Reader.open_ ~filename in
  let first = ref true in
  let n_long = ref 0 in
  let last_ext = Array.make 256 None in
  let ext_samples = ref 0 in
  (* Ignore any samples arising from spawn_traced_domains, or from domain initialisation *)
  let should_ignore_alloc domain (bt : Memtrace.Trace.Location_code.t array) =
    let has_frame name =
      bt
      |> Array.exists (fun loc ->
        Memtrace.Trace.Reader.lookup_location_code r loc
        |> List.exists (fun (loc : Memtrace.Trace.Location.t) ->
          String.ends_with loc.defname ~suffix:name))
    in
    has_frame "spawn_traced_domain"
    || (domain <> Memtrace.Trace.Domain_id.main_domain && not (has_frame "dom_func"))
  in
  let module Obj_tbl = Memtrace.Trace.Obj_id.Tbl in
  let ignored_ids = Obj_tbl.create 10 in
  let seen_ids = Obj_tbl.create 10 in
  let dom1_ids = Obj_tbl.create 10 in
  let ext_ids = Obj_tbl.create 10 in
  Memtrace.Trace.Reader.iter r (fun _ ev ->
    (match ev with
     (* Check that Obj_ids are distinct and that each object has states: Alloc -> Promote?
        -> Collect *)
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
    match ev with
    | Alloc { length = 256; source = External; _ } -> () (* leaked_ext_token *)
    | Alloc info
      when should_ignore_alloc
             info.domain
             (Array.sub info.backtrace_buffer 0 info.backtrace_length) ->
      Memtrace.Trace.Obj_id.Tbl.add ignored_ids info.obj_id ()
    | (Promote (id, _) | Collect (id, _))
      when Memtrace.Trace.Obj_id.Tbl.mem ignored_ids id -> ()
    | Alloc ({ length = 4242; source = Major; _ } as info) ->
      assert (!first = true);
      first := false;
      assert (info.domain = Memtrace.Trace.Domain_id.main_domain);
      ()
    | Alloc ({ length = 1000; source = Major; _ } as info) ->
      (* backtraces should be truncated *)
      assert (info.backtrace_length > 3000 && info.backtrace_length < 4000);
      assert (info.domain = Memtrace.Trace.Domain_id.main_domain);
      incr n_long
    | Alloc ({ length = 7; source = External; _ } as info) ->
      Obj_tbl.add ext_ids info.obj_id ();
      last_ext.((info.domain :> int)) <- Some info.obj_id;
      ext_samples := !ext_samples + info.nsamples;
      validate_ext_alloc_backtrace r info.backtrace_buffer info.backtrace_length
    | Alloc ({ length = 397; source = Major; _ } as info) ->
      assert ((info.domain :> int) = (Domain.get_id (Option.get domain) :> int));
      Memtrace.Trace.Obj_id.Tbl.add dom1_ids info.obj_id ();
      ()
    | Collect (id, _) when Memtrace.Trace.Obj_id.Tbl.mem dom1_ids id ->
      Memtrace.Trace.Obj_id.Tbl.remove dom1_ids id
    | Collect (id, dom) when Obj_tbl.mem ext_ids id ->
      assert (last_ext.((dom :> int)) = Some id);
      last_ext.((dom :> int)) <- None
    | e ->
      failwith
        ("unexpected "
         ^ Memtrace.Trace.Event.to_string (Memtrace.Trace.Reader.lookup_location_code r) e
        ));
  Memtrace.Trace.Reader.close r;
  Unix.unlink filename;
  let n_domains =
    1
    +
    match domain with
    | Some _ -> 1
    | None -> 0
  in
  assert (650 <= !ext_samples / n_domains && !ext_samples / n_domains < 750);
  assert (not !first);
  assert (!n_long = 20)
;;

let () =
  Random.self_init ();
  (* Random.bool () initialises some state on first use. To avoid tracing this, use it
     now. *)
  let _ : bool = Sys.opaque_identity (Random.bool ()) in
  for _i = 1 to 10 do
    go ()
  done
;;
