open Memtrace.Trace
module JSON = Yojson.Basic


type event_begin = {
  first_sample: int;
  start_time: Timedelta.t;
  locations: string list;
  depth: int;
}

type event_end = {
  last_sample: int;
  end_time: Timedelta.t;
}


let rec take_rev n acc = function
  | [] -> acc
  | _ when n = 0 -> acc
  | x :: xs -> take_rev (n-1) (x :: acc) xs
let take_rev n xs = take_rev n [] xs


let too_short estart eend =
  let nsamples = eend.last_sample - estart.first_sample + 1 in
  nsamples < 5

let too_recursive evs ev =
  let top = take_rev 10 evs in
  List.exists (fun (_,ev') -> ev'.locations = ev.locations) top

let ploc ppf loc =
  List.iter (fun l -> Printf.fprintf ppf " %s" l) loc

let emit_event_start (info : Info.t) ev =
  (*Printf.printf "%*s<%a\n" (ev.depth*2) "" ploc ev.locations*)
  let json = [
    "name", `String (String.concat "/" ev.locations);
    "ph", `String "B";
    "ts", `Float (Int64.to_float (Timedelta.to_int64 ev.start_time));
    "pid", `Int (Int64.to_int info.pid);
    "tid", `Int (Int64.to_int info.pid);
  ] in
  print_string ","; JSON.to_channel stdout (`Assoc json); print_string "\n"
    
let emit_event_end (info : Info.t) ev eend =
  (* let duration = (Int64.sub (Timedelta.to_int64 eend.end_time) (Timedelta.to_int64 ev.start_time)) in
  let nsamples = eend.last_sample - ev.first_sample in
  Printf.printf "%*s>%a [%Ld %d]\n" (ev.depth*2) "" ploc ev.locations duration nsamples *)
  let json = [
    "name", `String (String.concat "/" ev.locations);
    "ph", `String "E";
    "ts", `Float (Int64.to_float (Timedelta.to_int64 eend.end_time));
    "pid", `Int (Int64.to_int info.pid);
    "tid", `Int (Int64.to_int info.pid);
  ] in
  print_string ","; JSON.to_channel stdout (`Assoc json); print_string "\n"


let dump filename =
  let trace = Reader.open_ ~filename in
  let prev_time = ref None in
  let sample_id = ref 0 in
  let active_events = ref [] in
  let nevents = ref 0 in
  let nshort = ref 0 in     (* events in active_events not started because possibly too short *)
  let info = Reader.info trace in

  let process prev_time time backtrace_buffer common_prefix = 
    assert (!nshort <= !nevents);

    while !nevents < common_prefix do
      let locations =
        backtrace_buffer.(!nevents)
        |> Reader.lookup_location_code trace
        |> List.map (fun l -> l.Location.defname) in
      let ev = { depth = !nevents; first_sample = !sample_id - 1; start_time = prev_time; locations } in
      active_events := (false, ev) :: !active_events;
      incr nevents;
      incr nshort;
    done;
    
    let eend = { last_sample = !sample_id; end_time = time } in
    while !nevents > common_prefix &&
          !nshort > 0 &&
          too_short (snd (List.hd !active_events)) eend do
      decr nevents;
      decr nshort;
      assert (fst (List.hd !active_events) = false);
      active_events := List.tl !active_events;
    done;
    
    if !nevents > common_prefix then begin
      let rec emit_starts n evs =
        if n = 0 then evs else
        match evs with
        | [] | (true, _) :: _  -> assert false
        | (false, ev) :: evs ->
           let evs = emit_starts (n-1) evs in
           assert (not (too_short ev eend));
           if not (too_recursive evs ev) then
             (emit_event_start info ev; (true, ev) :: evs)
           else
             (false, ev) :: evs
      in
      (* FIXME skip too_recursive entries *)
      active_events := emit_starts !nshort !active_events;
      nshort := 0;
      while !nevents > common_prefix do
        decr nevents;
        let started, estart = List.hd !active_events in
        if started then emit_event_end info estart eend;
        active_events := List.tl !active_events;
      done;
    end
  in

  print_string "[\n";
  begin
    let json = [
      "name", `String "init";
      "ph", `String "X";
      "ts", `Float 0.;
      "pid", `Int (Int64.to_int info.pid);
      "tid", `Int (Int64.to_int info.pid);
    ] in
    JSON.to_channel stdout (`Assoc json); print_string "\n"
  end;

  Reader.iter trace (fun time ev ->
    begin match !prev_time, ev with
    | _, (Collect _ | Promote _)
    | None, _ -> ()
    | Some prev_time, Alloc {backtrace_buffer; common_prefix; _} ->
       process prev_time time backtrace_buffer common_prefix
    end;
    incr sample_id;
    prev_time := Some time);
  begin match !prev_time with
  | None -> assert (!nevents = 0)
  | Some time -> process time time [| |] 0
  end;
  Reader.close trace;
  print_string "]\n"



let () =
  if Array.length Sys.argv <> 2 then
    Printf.fprintf stderr "Usage: %s <trace file>\n" Sys.executable_name
  else
    dump Sys.argv.(1)
