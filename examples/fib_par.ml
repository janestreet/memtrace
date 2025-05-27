(* fib_par.ml *)
let num_domains = try int_of_string Sys.argv.(1) with _ -> 1
let n = try int_of_string Sys.argv.(2) with _ -> 1

(* Sequential Fibonacci *)
let rec fib n =
  if n < 2 then 1 else fib (n - 1) + fib (n - 2)

module T = Domainslib.Task

let rec fib_par pool n =
  let _ = Buffer.create 10000 in
  if n > 20 then begin
      let a = T.async pool (fun _ -> fib_par pool (n-1)) in
      let b = T.async pool (fun _ -> fib_par pool (n-2)) in
      T.await pool a + T.await pool b
    end else
    (* Call sequential Fibonacci if the available work is small *)
    fib n

let main () =
  Memtrace.trace_if_requested ~context:"fib" ();
  let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
  let res = T.run pool (fun _ -> fib_par pool n) in
  T.teardown_pool pool;
  Printf.printf "fib(%d) = %d\n" n res

let _ = main ()