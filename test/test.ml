open Memtrace.Trace

let with_temp f =
  let s = Filename.temp_file "memtrace" "ctf" in
  let fd = Unix.openfile s [O_RDWR] 0o600 in
  Fun.protect
    ~finally:(fun () -> Unix.close fd; Unix.unlink s)
    (fun () -> f fd)

let mkloc filename line start_char end_char defname =
  { filename; line; start_char; end_char; defname }

let locations =
  [| [mkloc "foo.ml" 42 100 120 "func"];
     [mkloc "apiosdjfoaijsdf.ml" 100 1000 1023 "aiosjdf"];
     [mkloc "apiosdjfoaijsdf.ml" 19 1000 1023 "aiosjdf"; mkloc "inline" 1 1 1 "fjkisda"];
  |]

let id : int -> obj_id = Obj.magic

let info = {
  sample_rate = 0.01;
  word_size = 64;
  executable_name = "exec";
  host_name = "host";
  ocaml_runtime_params = "runtime";
  pid = 42L;
  start_time = 23897423L
}

let events =
  [ 0, Alloc {obj_id = id 0;
              length = 42;
              nsamples = 1;
              is_major = false;
              backtrace_buffer = [| 0; 1 |];
              backtrace_length = 2;
              common_prefix = 0};
    1, Alloc {obj_id = id 1;
              length = 2;
              nsamples = 1;
              is_major = false;
              backtrace_buffer = [| 0; 1; 2 |];
              backtrace_length = 3;
              common_prefix = 2};
    100, Alloc {obj_id = id 2;
              length = 2;
              nsamples = 1;
              is_major = false;
              backtrace_buffer = [| 0; 1; 2 |];
              backtrace_length = 3;
              common_prefix = 3};
    101, Collect (id 1);
    102, Promote (id 2)
  ]

let copy_event = function
  | Alloc ev ->
     Alloc { ev with backtrace_buffer = 
       Array.sub ev.backtrace_buffer 0 ev.backtrace_length  }
  | ev -> ev

let test () = with_temp @@ fun fd ->
  let w = make_writer fd info in
  events |> List.iter (fun (i, ev) ->
     let now = Int64.(add info.start_time (of_int (i * 1_000_000))) in
     put_event w ~decode_callstack_entry:(fun bt i -> locations.(bt.(i)))
       now ev);
  flush w;
  Unix.lseek fd 0 SEEK_SET |> ignore;
  let r = make_reader fd in
  assert (trace_info r = info);
  let evs = ref [] in
  iter_trace r ~parse_backtraces:true (fun td ev ->
    (* assert (Int64.rem td 1_000_000L = 0L); *)
    evs := ((Int64.(to_int (div td 1_000_000L)), copy_event ev) :: !evs));
  let decode_loc l = locations.(l) in
  let rec compare exp act =
    match exp, act with
    | [], [] -> ()
    | exp :: exps, act :: acts when exp = act ->
       compare exps acts
    | [], (tact, act) :: _ ->
       Printf.printf "Extra event decoded:\n  %d. %s\n%!"
         tact
         (string_of_event decode_loc act);
       failwith "Extra events"
    | (texp, exp) :: _, [] ->
       Printf.printf "Missing event:\n  %d. %s\n%!"
         texp
         (string_of_event decode_loc exp);
       failwith "Missing events"
    | (texp, exp) :: _, (tact, act) :: _ ->
       Printf.printf "Event doesn't match. Expected:\n  %d. %s\nbut decoded:\n  %d. %s\n%!"
         texp (string_of_event decode_loc exp)
         tact (string_of_event decode_loc act);
       failwith "Incorrect event"
  in
  compare events (List.rev !evs)

let () = test ()
