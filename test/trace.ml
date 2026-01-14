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
  (* Make sure the backtrace has been scrubbed of calls to
     [ext_alloc] and [ext_alloc_slowpath] in [memprof_tracer.ml] *)
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

let go () =
  let filename = Filename.temp_file "memtrace" "ctf" in
  (* Check that we can recover from a bad sampling rate *)
  (match Memtrace.start_tracing ~context:(Some "ctx") ~sampling_rate:42.0 ~filename with
   | _ -> failwith "should have failed"
   | exception _ -> ());
  (* Verify that non-initial domains aren't traced *)
  let dom_func () =
    for _i = 1 to 10 do
      let _ : int array = Array.make 500 42 |> Sys.opaque_identity in
      match Memtrace.External.alloc ~bytes:50 with
      | Null -> ()
      | This _ -> assert false
    done
  in
  let _ : _ = Sys.opaque_identity dom_func in
  let t = Memtrace.start_tracing ~context:(Some "ctx") ~sampling_rate:0.1 ~filename in
  let spawn_traced_domain () =
    (Domain.Safe.spawn [@inlined never] [@alert "-do_not_spawn_domains"])
      (Obj.magic_portable dom_func) [@nontail]
  in
  let domain =
    if Domain.recommended_domain_count () <= 1
    then None
    else Some (spawn_traced_domain ())
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
  Memtrace.stop_tracing t;
  Option.iter Domain.join domain;
  let r = Memtrace.Trace.Reader.open_ ~filename in
  let first = ref true in
  let n_long = ref 0 in
  let last_ext = ref None in
  let ext_samples = ref 0 in
  (* Ignore any samples arising from spawn_traced_domains *)
  let should_ignore_backtrace (bt : Memtrace.Trace.Location_code.t array) =
    bt
    |> Array.exists (fun loc ->
      Memtrace.Trace.Reader.lookup_location_code r loc
      |> List.exists (fun (loc : Memtrace.Trace.Location.t) ->
        String.ends_with loc.defname ~suffix:"spawn_traced_domain"))
  in
  let ignored_ids = Memtrace.Trace.Obj_id.Tbl.create 10 in
  Memtrace.Trace.Reader.iter r (fun _ ev ->
    match ev with
    | Alloc info
      when should_ignore_backtrace
             (Array.sub info.backtrace_buffer 0 info.backtrace_length) ->
      Memtrace.Trace.Obj_id.Tbl.add ignored_ids info.obj_id ()
    | (Promote (id, _) | Collect (id, _))
      when Memtrace.Trace.Obj_id.Tbl.mem ignored_ids id -> ()
    | Alloc info when !first ->
      first := false;
      assert (info.length = 4242);
      ()
    | Alloc info when info.length = 1000 ->
      (* backtraces should be truncated *)
      assert (info.backtrace_length > 3500 && info.backtrace_length < 4000);
      incr n_long
    | Alloc info when info.length = 7 ->
      last_ext := Some info.obj_id;
      ext_samples := !ext_samples + info.nsamples;
      validate_ext_alloc_backtrace r info.backtrace_buffer info.backtrace_length
    | Collect (id, _) ->
      assert (!last_ext = Some id);
      last_ext := None
    | e ->
      failwith
        ("unexpected "
         ^ Memtrace.Trace.Event.to_string (Memtrace.Trace.Reader.lookup_location_code r) e
        ));
  Memtrace.Trace.Reader.close r;
  Unix.unlink filename;
  assert (650 <= !ext_samples && !ext_samples < 750);
  assert (not !first);
  assert (!n_long = 20)
;;

let () =
  go ();
  go ()
;;
