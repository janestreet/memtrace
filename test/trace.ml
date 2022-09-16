let check_errors () =
  Unix.putenv "MEMTRACE" "/bad/file/name";
  (match Memtrace.trace_if_requested () with
   | _ -> assert false
   | exception (Invalid_argument _) -> ());
  Unix.putenv "MEMTRACE" "/tmp/goodfilename";
  (match Memtrace.trace_if_requested ~sampling_rate:(-3.) () with
   | _ -> assert false
   | exception (Invalid_argument _) -> ());
  Unix.putenv "MEMTRACE" "/tmp/goodfilename";
  Unix.putenv "MEMTRACE_RATE" "42";
  (match Memtrace.trace_if_requested () with
   | _ -> assert false
   | exception (Invalid_argument _) -> ());
  Unix.putenv "MEMTRACE" "/tmp/goodfilename";
  Unix.putenv "MEMTRACE_RATE" "potato";
  (match Memtrace.trace_if_requested () with
   | _ -> assert false
   | exception (Invalid_argument _) -> ())

let () = check_errors ()

let globs = Array.make 1000 [| |]
let nglobs = ref 0
let leak x = globs.(!nglobs) <- x; incr nglobs

let rec long_bt = function
  | 0 ->
     leak (Array.make 1000 0);
     (Sys.opaque_identity List.iter) (fun () ->
       leak (Array.make 1000 0)) [()];
     42
  | n ->
    if Random.bool () then
      1 + long_bt (n-1)
    else
      2 + long_bt (n-1)

let go () =
  let filename = Filename.temp_file "memtrace" "ctf" in
  let t = Memtrace.start_tracing_with_gc_events ~context:"ctx" ~sampling_rate:0.1 ~filename () in
  Gc_recent_events.For_testing.write_test_event ();
  Gc_recent_events.For_testing.write_test_event ();
  leak (Array.make 4242 42);
  for _i = 1 to 10 do
    let n = long_bt 10_000 in
    assert (n > 0);
  done;
  for _i = 1 to 1000 do
    Option.iter Memtrace.External.free
      (Memtrace.External.alloc ~bytes:((Sys.word_size / 8) * 7))
  done;
  Memtrace.stop_tracing t;
  let r = Memtrace.Trace.Reader.open_ ~filename in
  let first = ref true in
  let n_long = ref 0 in
  let last_ext = ref None in
  let ext_samples = ref 0 in
  let gc_events = ref 0 in
  Memtrace.Trace.Reader.iter r (fun _ ev ->
    match ev with
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
    | Collect id ->
      assert (!last_ext = Some id);
      last_ext := None
    | Gc_event e ->
      assert (e.data = Test);
      incr gc_events
    | e ->
      failwith ("unexpected " ^ (Memtrace.Trace.Event.to_string
                                   (Memtrace.Trace.Reader.lookup_location_code r) e)));
  Memtrace.Trace.Reader.close r;
  Unix.unlink filename;
  assert (650 <= !ext_samples && !ext_samples < 750);
  assert (not !first);
  assert (!n_long = 20);
  assert (!gc_events = 2)

let gc_event_test () =
  let filename = Filename.temp_file "memtrace" "ctf" in
  let t = Memtrace.start_tracing_with_gc_events ~context:"ctx" ~sampling_rate:0.1 ~filename () in
  let xs = List.init 10000 (fun x -> x + 1) in
  let s = List.fold_left (+) 0 xs in
  Gc.minor ();
  Gc.major ();
  Memtrace.stop_tracing t;
  assert (s = 50005000);
  let has_minor = ref false in
  let has_major = ref false in
  let r = Memtrace.Trace.Reader.open_ ~filename in
  Memtrace.Trace.Reader.iter r (fun _ ev ->
    match ev with
    | Gc_event { data; begin_; end_ } ->
      assert (begin_ <= end_);
      (match data with
      | Minor _ -> has_minor := true
      | Major_slice _ ->
        assert (!has_minor);
        has_major := true
      | _ -> ())
    | _ -> ());
  assert (!has_major)


let () =
  go (); go ();
  gc_event_test ()
