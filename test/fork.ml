let test_fork ~quick_exit () =
  let filename = Filename.temp_file "memtrace" "ctf" in
  (Unix.putenv [@ocaml.alert "-unsafe_multidomain"]) "MEMTRACE" filename;
  let tr = Memtrace.start_tracing ~context:None ~sampling_rate:1. ~filename in
  let alloc_before = 1234
  and alloc_after = 7364
  and alloc_child = 42 in
  let _ : string array = Sys.opaque_identity Array.make alloc_before "a" in
  (match Unix.fork () with
   | 0 ->
     let count = if quick_exit then 1 else 1000000 in
     for _i = 1 to count do
       ignore (Sys.opaque_identity Array.make alloc_child "a" : string array)
     done;
     exit 0
   | pid ->
     (match Unix.waitpid [] pid with
      | _, WEXITED 0 -> ()
      | _ -> assert false));
  let _ : string array = Sys.opaque_identity Array.make alloc_after "a" in
  Memtrace.stop_tracing tr;
  let module R = Memtrace.Trace.Reader in
  let tr = R.open_ ~filename in
  let sizes = Hashtbl.create 20 in
  R.iter tr (fun _time ev ->
    match ev with
    | Alloc a -> Hashtbl.add sizes a.length ()
    | _ -> ());
  assert (Hashtbl.mem sizes alloc_before);
  assert (Hashtbl.mem sizes alloc_after);
  assert (not (Hashtbl.mem sizes alloc_child));
  ()
;;

let () = test_fork ~quick_exit:false ()
let () = test_fork ~quick_exit:true ()
