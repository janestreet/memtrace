(* Regression test for a deferred callback raising.

   If thread A holds the lock while thread B calls [with_lock_deferred], B's callback is
   queued and later run by A's [unlock]. If that callback raises, the failure must be
   reported via [on_error] and poison the lock, but must not raise from A's [unlock]: A
   may be inside a memprof callback (or any other innocent context), and the exception
   would propagate into unrelated user code. *)

module Lock = Memtrace__Lock

exception Callback_failure

let rec spin_until cond =
  if not (Atomic.get cond)
  then (
    Thread.yield ();
    spin_until cond)
;;

let () =
  let errors_reported = Atomic.make 0 in
  let lock =
    Lock.create
      ~on_error:(fun () exn ->
        match exn with
        | Callback_failure -> Atomic.incr errors_reported
        | _ -> assert false)
      (fun () -> ())
  in
  let locked = Atomic.make false in
  let deferred_queued = Atomic.make false in
  let holder_result = ref `Pending in
  let holder =
    Thread.create
      (fun () ->
        match
          Lock.with_lock_blocking lock ~f:(fun () ->
            Atomic.set locked true;
            (* Hold the lock until the raising callback has been queued, so that our
               [unlock] is the one that runs it *)
            spin_until deferred_queued)
        with
        | () -> holder_result := `Returned
        | exception exn -> holder_result := `Raised exn)
      ()
  in
  spin_until locked;
  (* Queued behind the holder; runs during the holder's [unlock] *)
  Lock.with_lock_deferred lock ~f:(fun () -> raise Callback_failure);
  Atomic.set deferred_queued true;
  Thread.join holder;
  (* The failure must not escape the thread that drained the deferred queue *)
  (match !holder_result with
   | `Returned -> ()
   | `Pending -> assert false
   | `Raised exn ->
     Printf.printf
       "deferred callback failure escaped unlock: %s\n"
       (Printexc.to_string exn);
     exit 1);
  (* The failure must have been reported exactly once via [on_error] *)
  assert (Atomic.get errors_reported = 1);
  (* The lock must be poisoned: blocking use raises [Stopped]... *)
  (match Lock.with_lock_blocking lock ~f:(fun () -> ()) with
   | () -> assert false
   | exception Lock.Stopped -> ());
  (* ...and the async-safe operations do nothing, without raising *)
  assert (Lock.try_lock lock ~f:(fun () -> ()) = None);
  Lock.with_lock_deferred lock ~f:(fun () -> assert false);
  assert (Atomic.get errors_reported = 1);
  print_endline "lock_test OK"
;;
