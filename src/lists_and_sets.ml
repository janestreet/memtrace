let () =
  Memtrace.trace_until_exit ~sampling_rate:0.001 ~filename:"memtrace.ctf"

let[@inline always] asdf i = ((i * 483205) land 0xfffff, i)
let[@inline always] mul i = let m = asdf i in assert (i >= 0); m
module S = Set.Make (struct type t = (int * int) option let compare = compare end)
let go () =
  List.init 10_000 (fun i -> Some (if i < -100 then assert false else mul i))
  |> List.map (fun x -> (*Unix.sleepf 0.001;*) S.singleton x)
  |> List.fold_left S.union S.empty


let () =
  go () |> Sys.opaque_identity |> ignore;
  Gc.full_major ()
