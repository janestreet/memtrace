open Memtrace

module Loc_tbl = Hashtbl.Make (struct
  type t = location_code
  let hash (x : location_code) = Int64.(shift_right (mul (x :> int64) 984372984721L) 17 |> to_int)
  let equal (x : location_code) (y : location_code) = Int64.equal (x :> int64) (y :> int64)
end)

module Str_tbl = Hashtbl.Make (struct
  type t = string
  let hash = Hashtbl.hash
  let equal = String.equal
end)

type loc_entry = {
  line: int;
  start_ch: int;
  end_ch: int;
  func: func;
  mutable alloc_count: int;
}
and func = {
  id : int;
  name : string;
  filename : string;
  mutable locs : loc_entry list;
  mutable total_count : int;

  mutable n_allocs : int;
  mutable total_dist_to_alloc : int;
}

module Func_tbl = Hashtbl.Make (struct
  type t = func
  let hash (f : func) = f.id * 49032809481
  let equal (f : func) (g : func) = f.id = g.id
end)

let total_allocs (f : func) =
  List.fold_left (fun k e -> k + e.alloc_count) f.total_count f.locs

let direct_allocs (f : func) =
  List.fold_left (fun k e -> k + e.alloc_count) 0 f.locs

let avg_dist_to_alloc (f : func) =
  float_of_int f.total_dist_to_alloc /. float_of_int f.n_allocs

type loc_table = {
  entries : loc_entry Loc_tbl.t;
  funcs : (string * string, func) Hashtbl.t;
  trace : Memtrace.trace;
  mutable next_id : int;
}

let new_loc_table trace =
  { entries = Loc_tbl.create 10000;
    funcs = Hashtbl.create 10000;
    trace;
    next_id = 0 }

let add_loc t loc =
  match Loc_tbl.find t.entries loc with
  | e -> e
  | exception Not_found ->
     let filename, funcname, line, start_ch, end_ch =
       match lookup_location t.trace loc with
       | [] -> "??", Printf.sprintf "#%Lx" (loc :> int64), 0, 0, 0
       | locs ->
          let l = (List.nth locs (List.length locs - 1)) in
          l.filename, l.defname, l.line, l.start_char, l.end_char in
     let func =
       match Hashtbl.find t.funcs (filename, funcname) with
       | func ->
          func
       | exception Not_found ->
          let id = t.next_id in
          t.next_id <- t.next_id + 1;
          let func : func = { id; filename; name = funcname; locs = [];
                              total_count = 0; n_allocs = 0; total_dist_to_alloc = 0 } in
          Hashtbl.add t.funcs (filename, funcname) func;
          func in
     let entry =
       { line; start_ch; end_ch; func; alloc_count = 0 } in
     Loc_tbl.add t.entries loc entry;
     func.locs <- entry :: func.locs;
     entry

module HH = Heavy_hitters.Make(struct
  type t = func * func
  let hash ((a, b) : t) =
    a.id * 1231441 + b.id * 3821
  let equal ((a,b) : t) ((a',b') : t) = (a.id = a'.id) && (b.id = b'.id)
(*
  let hash ((a : location_code), (b : location_code)) =
    Int64.(shift_right (add (mul (a :> int64) 0x94837298472a9321L) (mul (b :> int64) 0x4783213feac37L)) 11 |> to_int)
  let equal (a, b) (a', b') =
    Int64.equal (a : location_code :> int64) (a' : location_code :> int64) &&
      Int64.equal (b : location_code :> int64) (b' : location_code:> int64)*)
end)


let count filename =
  let trace = open_trace ~filename in
  let last = ref [| |] in
  let hh = HH.make 10000 in
  let seen = Func_tbl.create 100 in
  let locs = new_loc_table trace in
  let total_samples = ref 0 in
  iter_trace trace (fun _time ev ->
      match ev with
      | Alloc {obj_id=_; length=_; nsamples; is_major=_; common_prefix; new_suffix} ->
         let bt = Array.concat [Array.sub !last 0 common_prefix; Array.of_list new_suffix] in
         last := bt;
         let allocpt = add_loc locs bt.(Array.length bt - 1) in
         allocpt.alloc_count <- allocpt.alloc_count + nsamples;
         Func_tbl.clear seen;
         for i' = 0 to Array.length bt - 2 do
           let i = Array.length bt - 2 - i' in
           let b = (add_loc locs bt.(i)).func in
           if not (Func_tbl.mem seen b) then begin
               Func_tbl.add seen b ();
               b.total_count <- b.total_count + nsamples;
               b.n_allocs <- b.n_allocs + 1;
               b.total_dist_to_alloc <- b.total_dist_to_alloc + (Array.length bt - 1 - i);
               HH.add hh (b, allocpt.func)
             end
         done;
         total_samples := !total_samples + nsamples
      | Promote _ -> ()
      | Collect _ -> ());
  close_trace trace;
  let total_samples = !total_samples in
  let hot_allocs = Func_tbl.create 100 in
  HH.iter hh (fun (fn,al) d1 _d2 ->
      if 1000 * direct_allocs al / total_samples > 5 then begin
          let callers =
            match Func_tbl.find hot_allocs al with
            | c -> c
            | exception Not_found ->
               let c = ref [] in
               Func_tbl.add hot_allocs al c;
               c in
          let pair_freq = d1 in
          let tot_freq = total_allocs fn in
          if 100 * pair_freq / tot_freq > 20 then begin
              callers := (fn, pair_freq) :: !callers
            end
        end);
  let hot_allocs = Func_tbl.fold (fun al c acc -> (al, !c) :: acc) hot_allocs [] in
  let hot_allocs = List.sort (fun (al,_) (al',_) -> compare (direct_allocs al') (direct_allocs al)) hot_allocs in
  hot_allocs |> List.iter (fun (al, callers) ->
                    let freq = float_of_int (direct_allocs al) /. float_of_int total_samples in 
                    Printf.printf "%4.1f%% %s" (100. *. freq) al.name;
                    Printf.printf " (%s:" al.filename;
                    let printed = ref 0 in
                    let locs = al.locs |> List.sort (fun l1 l2 -> compare l2.alloc_count l1.alloc_count) in
                    locs |> List.iter (fun { line; start_ch; end_ch; alloc_count; func=_ } ->
                                if alloc_count > 0 then begin
                                    begin match !printed with
                                    | n when n < 3 ->
                                       if n > 0 then Printf.printf ", ";
                                       Printf.printf "%d:%d-%d" line start_ch end_ch
                                    | 3 -> Printf.printf "..."
                                    | _ -> () end;
                                    incr printed
                                  end);
                    Printf.printf ")\n";
                    let first_caller = ref true in

                    let callers = List.sort (fun (c,f) (c',f') ->
                                      (*compare (avg_dist_to_alloc c) (avg_dist_to_alloc c')*)
                                      compare (float_of_int f' /. float_of_int (total_allocs c')) (float_of_int f /. float_of_int (total_allocs c))
                                    ) callers in
                    callers |> List.iter (fun (caller,freq) ->
                                   let rfreq = float_of_int freq /. float_of_int (total_allocs caller) in
                                   let pfreq = float_of_int freq /. float_of_int total_samples in
                                   if float_of_int (total_allocs caller) /. float_of_int total_samples > 0.005 then begin
                                     if !first_caller then Printf.printf "      accounting for:\n";
                                     first_caller := false;
                                     Printf.printf " %3.0f%% of %s (%.1f%%)\n" (100. *. rfreq) caller.name (100. *. pfreq)
                                   end);
                    Printf.printf "\n")
      

(*
    let pair_freq = d1 in
    let tot_freq = summarize_fn fn in
    if 100 * pair_freq / tot_freq > 20 then begin
      let frac_of_program = float_of_int pair_freq /. float_of_int !total_samples in
      Printf.printf "%.1f%% % 6d % 6d % 6d %d %s %s\n" (100. *. rf) !total_samples d1 d2 tot_freq fn.name al.name
    end)
 *)

let () =
  if Array.length Sys.argv <> 2 then
    Printf.fprintf stderr "Usage: %s <trace file>\n" Sys.executable_name
  else
    count Sys.argv.(1)
