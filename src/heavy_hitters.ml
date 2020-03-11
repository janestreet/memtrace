module Make(A : Hashtbl.HashedType) = struct

module Tbl = Hashtbl.Make(A)

type counter = {
  (* (hits - misses) is the usual Misra-Gries summary *)
  mutable hits : int;
  mutable misses : int;
  (* number of elements added before this one (including duplicates) *)
  added_before : int;
  (* number of elements skipped before this one was added *)
  skipped_before : int;
}

(*let upper_bound t c =
  c.skipped_before 
 *)
type t = {
  k : int;
  tbl : counter Tbl.t;
  mutable len : int;
  mutable added : int;
  mutable skipped : int;
}

let make k =
  {
    k;
    tbl = Tbl.create k;
    len = 0;
    added = 0;
    skipped = 0;
  }

let _check t =
  let hits = ref 0 in
  t.tbl |> Tbl.iter (fun _k c -> hits := !hits + c.hits);
  assert (t.len = Tbl.length t.tbl);
  assert (!hits + t.skipped = t.added);
  ()

let add t x =
(*  check t;*)
  begin match Tbl.find_opt t.tbl x with
  | Some c ->
     c.hits <- c.hits + 1
  | None ->
     if t.len < t.k then begin
       t.len <- t.len + 1;
       Tbl.add t.tbl x { hits = 1; misses = 0; added_before = t.added; skipped_before = t.skipped }
     end else begin
       t.skipped <- t.skipped + 1;
       t.tbl |> Tbl.filter_map_inplace (fun _k c ->
         c.misses <- c.misses + 1;
         if c.hits > c.misses then Some c else begin
           t.len <- t.len - 1;
           t.skipped <- t.skipped + c.hits;
           None
         end)
     end
  end;
  t.added <- t.added + 1

let length t = t.added

let iter t f =
  t.tbl |> Tbl.iter (fun x c -> f x c.hits c.skipped_before)

end
