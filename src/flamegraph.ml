module StrTbl = Hashtbl.Make(struct type t = string let equal = String.equal let hash = Hashtbl.hash end)
type summary = {
  mutable samples: int;
  subsums : summary StrTbl.t;
}

let summary filename =
  let last = ref [| |] in
  let summary = { samples = 0; subsums = StrTbl.create 20 } in
  let count (filenames, nsamples) =
    let lastsum =
      List.fold_left (fun sum f ->
        if StrTbl.mem sum.subsums f then
          StrTbl.find sum.subsums f
        else begin
          let s = { samples = 0; subsums = StrTbl.create 10 } in
          StrTbl.add sum.subsums f s;
          s
        end) summary filenames in
    lastsum.samples <- lastsum.samples + nsamples in

  let allocs = Hashtbl.create 20 in
  let sz = ref 0 in
  let nallocs = ref 0 in
  let open Memtrace in
  let trace = open_trace ~filename in
  iter_trace trace (fun _time ev ->
    match ev with
  | Alloc {obj_id; length=_; nsamples; is_major=_; common_prefix; new_suffix} ->
    let bt = Array.concat [Array.sub !last 0 common_prefix; Array.of_list new_suffix] in
    last := bt;
    let str_of_location { filename; line; start_char=_; end_char=_  } =
      Printf.sprintf "%s:%d" filename line in
    let _print_location ppf { filename; line; start_char; end_char  } =
      Printf.fprintf ppf "%s:%d:%d-%d" filename line start_char end_char in
    let filenames = List.concat (bt |> Array.map (fun l ->
      let locs = lookup_location trace l in
      List.map (fun ({ filename=_; _ } as l) -> str_of_location l) locs) |> Array.to_list) in
    let seen = StrTbl.create 10 in
    let rec dedup = function
      | [] -> []
      | [x] -> [x]
      | x :: x' :: xs when x = x' -> dedup (x :: xs)
      | x :: xs -> x :: dedup xs in
    let filenames = dedup filenames in
    let first_filenames =
      filenames |> List.filter (fun f ->
        if StrTbl.mem seen f then false else begin
          StrTbl.add seen f ();
          true
        end) in
    Hashtbl.add allocs obj_id (first_filenames, nsamples);
    sz := !sz + common_prefix + List.length new_suffix;
    incr nallocs;
    if true then count (filenames, nsamples);
    (* count (first_filenames, nsamples) *)
(*    first_filenames |> List.iter (Printf.printf " %s");
      Printf.printf "\n%!"*)
  | Promote _ -> ()
  (*count (Hashtbl.find allocs i)*)
  | Collect i -> assert (Hashtbl.mem allocs i); Hashtbl.remove allocs i );
  Memtrace.close_trace trace;

  let rec dump_summary files_rev summary =
    if summary.samples > 0 then begin match List.rev files_rev with
    | [] -> ()
    | [_] -> ()
    | (x :: xs) ->
      Printf.printf "%s" x;
      List.iter (Printf.printf ";%s") xs;
      Printf.printf " %d\n" summary.samples
    end;
    let keys = StrTbl.fold (fun k _ ks -> k :: ks) summary.subsums [] |> List.sort String.compare in
    keys |> List.iter (fun f ->
      let s = StrTbl.find summary.subsums f in
      dump_summary (f :: files_rev) s) in
  dump_summary [] summary;
  Printf.fprintf stderr "sz/kb %d\nallocs %d\n" (!sz / 1024) !nallocs


let () =
  if Array.length Sys.argv <> 2 then
    Printf.fprintf stderr "Usage: %s <trace file>\n" Sys.executable_name
  else
    summary Sys.argv.(1)

