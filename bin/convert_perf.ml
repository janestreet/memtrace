module T = Memtrace.Trace
open Convert_perf_lexer

let copy infile outfile =
  let locations = T.Location_code.Tbl.create 20 in
  let decode_callstack_entry code =
    [T.Location_code.Tbl.find locations code] in
  let enter_location { addr; symbol; source } =
    let code : T.Location_code.t = Obj.magic (Int64.to_int addr) in
    let loc : T.Location.t =
      { filename = Filename.basename source;
        line = 0;
        start_char = 0;
        end_char = 0;
        defname = symbol } in
    if not (T.Location_code.Tbl.mem locations code) then
      T.Location_code.Tbl.add locations code loc;
    code in
  let rec go lexbuf trace = 
    match read_sample lexbuf with
    | exception End_of_file -> ()
    | { time; backtrace } ->
       let callstack = List.map enter_location backtrace |> Array.of_list in
       let timestamp = T.Timestamp.of_float time in
       let id = T.Writer.put_alloc trace
         timestamp
         ~length:1 ~nsamples:1 ~source:Major
         ~callstack
         ~decode_callstack_entry in
       T.Writer.put_collect trace timestamp id;
       go lexbuf trace in
  let info : T.Info.t = {
      sample_rate = 1.;
      word_size = 64;
      executable_name = "?";
      host_name = "?";
      ocaml_runtime_params = "?";
      pid = 0L;
      start_time = T.Timestamp.of_int64 0L;
      context = None } in
  let out = Unix.openfile outfile [O_WRONLY;O_CREAT;O_TRUNC] 0o600 in
  let trace = (T.Writer.create out ~getpid:(fun () -> 0L) info) in
  go (Lexing.from_channel (open_in infile)) trace;
  T.Writer.flush trace

let () =
  match Sys.argv with
  | [| _; infile; outfile |] -> copy infile outfile
  | _ -> Printf.printf "Usage: %s <input (perf script)> <output (memtrace)>\n" (Sys.argv.(0))
