open Memtrace

let matches s loc =
  loc |> List.exists (fun l -> l.defname = s)

let count filename target =
  let nfound = ref 0 and allocs = ref 0 in
  let trace = open_trace ~filename in
  let matched = Array.make 10_000 false in
  iter_trace trace (fun _time ev ->
    match ev with
  | Alloc {obj_id=_; length=_; nsamples; is_major=_; common_prefix; new_suffix} ->
    new_suffix |> List.iteri (fun i s ->
      matched.(common_prefix + i) <- matches target (lookup_location trace s));
    let found = ref false in
    for i = 0 to common_prefix + List.length new_suffix do
      if matched.(i) then found := true;
    done;
    allocs := !allocs + nsamples;
    if !found then nfound := !nfound + nsamples;
  | Promote _ -> ()
  | Collect _ -> ());
  close_trace trace;
  float_of_int !nfound  /. float_of_int !allocs


let () =
  if Array.length Sys.argv <> 3 then
    Printf.fprintf stderr "Usage: %s <trace file> <target>\n" Sys.executable_name
  else
    Printf.printf "%.1f%%\n" (100. *. count Sys.argv.(1) Sys.argv.(2))
