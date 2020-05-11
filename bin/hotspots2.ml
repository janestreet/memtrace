open Memtrace.Trace

module Hierarchical_heavy_hitters (X : Hashtbl.HashedType) : sig

  type t

  val create : float -> t

  val insert : t -> X.t list -> int -> unit

  val output : t -> float -> (X.t list * int * int * int) list * int

end = struct

  module Tbl = Hashtbl.Make(X)

  module Node = struct

    type t =
      { mutable count : int;
        mutable delta : int;
        mutable child_delta : int;
        children : t Tbl.t }

    let create () =
      let count = 0 in
      let delta = 0 in
      let child_delta = 0 in
      let children = Tbl.create 0 in
      { count; delta; child_delta; children }

    let find_or_create_child t x =
      match Tbl.find_opt t.children x with
      | Some t -> t
      | None ->
          let count = 0 in
          let delta = t.child_delta in
          let child_delta = delta in
          let children = Tbl.create 0 in
          let child = { count; delta; child_delta; children } in
          Tbl.add t.children x child;
          child

    let rec insert t xs count =
      match xs with
      | [] -> t.count <- t.count + count
      | x :: xs -> insert (find_or_create_child t x) xs count

    let compress t bucket =
      let rec loop parent t =
        Tbl.filter_map_inplace (fun _ child -> loop t child) t.children;
        let empty = Tbl.length t.children = 0 in
        let infrequent = t.count + t.delta <= bucket in
        if empty && infrequent then begin
          parent.count <- parent.count + t.count;
          parent.child_delta <- max parent.child_delta (t.count + t.delta);
          None
        end else begin
          Some t
        end
      in
      Tbl.filter_map_inplace (fun _ child -> loop t child) t.children

    let output t threshold =
      let cmp (_, (light_count1 : int), _, _) (_, (light_count2 : int), _, _) =
        compare light_count2 light_count1
      in
      let rec loop key t =
        let child_count, light_child_count, child_results =
          loop_children t
        in
        let results =
          List.map
            (fun (rest, light, lower, upper) -> (key :: rest, light, lower, upper))
            child_results
        in
        let count = t.count + child_count in
        let light_count = t.count + light_child_count in
        if light_count + t.delta > threshold then
          let results =
            List.merge cmp [[key], light_count, count, count + t.delta]
              results
          in
          count, 0, results
        else
          count, t.count + light_child_count, results
      and loop_children t =
        Tbl.fold
          (fun key child (c, f, r) ->
             let c', f', r' = loop key child in
             (c' + c, f' + f, List.merge cmp r' r))
          t.children (0, 0, [])
      in
      let _, _, results = loop_children t in
      results

  end

  type t =
    { root : Node.t;
      bucket_size : int;
      mutable current_bucket : int;
      mutable remaining : int;
      mutable total : int; }

  let create error =
    let root = Node.create () in
    let bucket_size = Float.to_int (Float.ceil (1.0 /. error)) in
    let current_bucket = 0 in
    let remaining = bucket_size in
    let total = 0 in
    { root; bucket_size; current_bucket; remaining; total }

  let insert t xs count =
    Node.insert t.root xs count;
    let remaining = t.remaining - 1 in
    if remaining > 0 then begin
      t.remaining <- remaining
    end else begin
      let current_bucket = t.current_bucket + 1 in
      Node.compress t.root current_bucket;
      t.current_bucket <- current_bucket;
      t.remaining <- t.bucket_size
    end;
    t.total <- t.total + 1

  let output t frequency =
    let threshold =
        Float.to_int (Float.floor (frequency *. (Float.of_int t.total)))
    in
    Node.output t.root threshold, t.total

end

module Location = struct

  type t = location_code

  let hash (x : location_code) = ((x :> int) * 984372984721) lsr 17
  let equal (x : location_code) (y : location_code) = (x = y)
end

module Loc_hitters = Hierarchical_heavy_hitters(Location)

module Loc_tbl = Hashtbl.Make(Location)

let error = 0.001

let wordsize = 8.  (* FIXME: store this in the trace *)

let print_bytes ppf = function
  | n when n < 100. ->
    Format.fprintf ppf "%4.0f B" n
  | n when n < 100. *. 1024. ->
    Format.fprintf ppf "%4.1f kB" (n /. 1024.)
  | n when n < 100. *. 1024. *. 1024. ->
    Format.fprintf ppf "%4.1f MB" (n /. 1024. /. 1024.)
  | n when n < 100. *. 1024. *. 1024. *. 1024. ->
    Format.fprintf ppf "%4.1f GB" (n /. 1024. /. 1024. /. 1024.)
  | n ->
    Format.fprintf ppf "%4.1f TB" (n /. 1024. /. 1024. /. 1024. /. 1024.)

let print_location ppf {filename; line; start_char; end_char; defname} =
  Format.fprintf ppf
    "%s (%s:%d:%d-%d)" defname filename line start_char end_char

let print_locations ppf locations =
  Format.pp_print_list ~pp_sep:Format.pp_print_space print_location
    ppf locations

let print_loc_code trace ppf loc =
  let locations = lookup_location trace loc in
  Format.pp_print_list ~pp_sep:Format.pp_print_space
    print_location ppf locations

let print_loc_codes trace ppf locs =
  Format.pp_print_list ~pp_sep:Format.pp_print_space
    (print_loc_code trace) ppf locs

let print_hitter trace tinfo total ppf (locs, _, count, _) =
  let freq = float_of_int count /. float_of_int total in
  let bytes = float_of_int count /. tinfo.sample_rate *. wordsize in
  match locs with
  | [] -> ()
  | [only] -> begin
      match lookup_location trace only with
      | [] -> ()
      | [alloc] ->
          Format.fprintf ppf "@[<v 4>%a (%4.1f%%) at %s (%s:%d:%d-%d)@]"
            print_bytes bytes (100. *. freq) alloc.defname
            alloc.filename alloc.line alloc.start_char alloc.end_char
      | alloc :: extra ->
          Format.fprintf ppf "@[<v 4>%a (%4.1f%%) at %s (%s:%d:%d-%d)@ %a@]"
            print_bytes bytes (100. *. freq) alloc.defname
            alloc.filename alloc.line alloc.start_char alloc.end_char
            print_locations extra
    end
  | first :: backtrace -> begin
      match lookup_location trace first with
      | [] -> ()
      | [alloc] ->
          Format.fprintf ppf "@[<v 4>%a (%4.1f%%) at %s (%s:%d:%d-%d)@ %a@]"
            print_bytes bytes (100. *. freq) alloc.defname
            alloc.filename alloc.line alloc.start_char alloc.end_char
            (print_loc_codes trace) backtrace
      | alloc :: extra ->
          Format.fprintf ppf "@[<v 4>%a (%4.1f%%) at %s (%s:%d:%d-%d)@ %a@ %a@]"
            print_bytes bytes (100. *. freq) alloc.defname
            alloc.filename alloc.line alloc.start_char alloc.end_char
            print_locations extra (print_loc_codes trace) backtrace
    end

let print_hitters trace tinfo total ppf hitters =
  let pp_sep ppf () =
    Format.pp_print_space ppf ();
    Format.pp_print_space ppf ()
  in
  Format.pp_print_list ~pp_sep
    (print_hitter trace tinfo total) ppf hitters

let print_report trace ppf (hitters, total) =
  let tinfo = trace_info trace in
  Format.fprintf ppf
    "@[<v 2>@ Trace for %s [%Ld]:@ %d samples of %a allocations@ \
     @[<v 2>@ %a@ @]@ "
    tinfo.executable_name tinfo.pid
    total
    print_bytes (float_of_int total /. tinfo.sample_rate *. wordsize)
    (print_hitters trace tinfo total) hitters

let count ~frequency ~filename =
  let trace = open_trace ~filename in
  let hhh = Loc_hitters.create error in
  let seen = Loc_tbl.create 100 in
  iter_trace trace
    (fun _time ev ->
       match ev with
       | Alloc {obj_id=_; length=_; nsamples; is_major=_;
                backtrace_buffer; backtrace_length; common_prefix=_} ->
           let rev_trace = ref [] in
           Loc_tbl.clear seen;
           for i = backtrace_length - 1 downto 0 do
             let loc = backtrace_buffer.(i) in
             if not (Loc_tbl.mem seen loc) then begin
               rev_trace := loc :: !rev_trace;
               Loc_tbl.add seen loc ()
             end
           done;
           Loc_hitters.insert hhh (List.rev !rev_trace) nsamples
      | Promote _ -> ()
      | Collect _ -> ());
  let results = Loc_hitters.output hhh frequency in
  Format.printf "%a" (print_report trace) results;
  close_trace trace

let default_frequency = 0.01

let () =
  if Array.length Sys.argv = 2 then begin
    count ~frequency:default_frequency ~filename:Sys.argv.(1)
  end else if Array.length Sys.argv = 3 then begin
    match Float.of_string Sys.argv.(2) with
    | frequency -> count ~frequency ~filename:Sys.argv.(1)
    | exception Failure _ ->
        Printf.fprintf stderr "Usage: %s <trace file> [frequency]\n"
          Sys.executable_name
  end else begin
    Printf.fprintf stderr "Usage: %s <trace file> [frequency]\n"
      Sys.executable_name
  end
