open Memtrace
let dump filename =
  let trace = open_trace ~filename in
  iter_trace trace (fun time ev ->
    Printf.printf "%010Ld " time;
    match ev with
  | Alloc {obj_id; length; nsamples; is_major; common_prefix; new_suffix} ->
    Printf.printf "%010d %s n=%d len=%d %d:" (obj_id :> int) (if is_major then "alloc_major" else "alloc") nsamples length common_prefix;
    let print_location ppf { filename; line; start_char; end_char  } =
      Printf.fprintf ppf "%s:%d:%d-%d" filename line start_char end_char in
    new_suffix |> List.iter (fun s ->
      lookup_location trace s |> List.iter (Printf.printf " %a" print_location));
    Printf.printf "\n%!"
  | Promote id ->
    Printf.printf "%010d promote\n" (id :> int)
  | Collect id ->
    Printf.printf "%010d collect\n" (id :> int));
  close_trace trace


let () =
  if Array.length Sys.argv <> 2 then
    Printf.fprintf stderr "Usage: %s <trace file>\n" Sys.executable_name
  else
    dump Sys.argv.(1)
