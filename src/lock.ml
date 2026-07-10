open Stdlib_shim

type 'a state =
  | Unlocked
  | Locked of ('a -> unit @ portable) list @@ once portable unyielding
  | Poisoned

type 'a pc = { pc : 'a @@ contended portable } [@@unboxed]

type ('a : value mod portable) t =
  { contents : 'a @@ contended
  ; state : 'a state pc Atomic.t @@ contended portable
  ; on_error : 'a -> exn -> unit @@ portable
  }

exception Stopped

let create ~on_error (f @ local once portable) =
  { contents = f (); state = Atomic.make { pc = Unlocked }; on_error }
;;

module Portable_list = struct
  let rec rev_append l1 l2 =
    match l1 with
    | [] -> l2
    | a :: l -> rev_append l (a :: l2)
  ;;

  let rev l = rev_append l []
end

let poison_lock ~lock exn =
  (* This is only called when we have exclusive access to lock.contents *)
  (try lock.on_error (Obj.magic_uncontended lock.contents) exn with
   | _exn ->
     (* Not much we can do here, and in general it's not safe to raise from here *)
     ());
  Atomic.set lock.state { pc = Poisoned }
;;

let apply_locked ~(f : (_ -> _ @ contended portable) @ portable) ~lock =
  (* This is only called when we have exclusive access to lock.contents *)
  f (Obj.magic_uncontended lock.contents)
;;

let rec unlock ~lock =
  match Atomic.get lock.state with
  | { pc = Locked [] } as locked_state ->
    if Atomic.compare_and_set lock.state locked_state { pc = Unlocked }
    then ()
    else unlock ~lock
  | { pc = Locked defers } as locked_state ->
    if Atomic.compare_and_set lock.state locked_state { pc = Locked [] }
    then run_deferred ~lock (Portable_list.rev defers)
    else unlock ~lock
  | _ -> assert false

and run_deferred ~lock : _ @ portable -> unit = function
  | [] -> unlock ~lock
  | f :: defers ->
    (match apply_locked ~f ~lock with
     | exception exn ->
       poison_lock ~lock exn;
       raise Stopped
     | () -> run_deferred ~lock defers)
;;

let rec lock_blocking ~lock =
  match Atomic.get lock.state with
  | { pc = Unlocked } as unlocked_state ->
    if Atomic.compare_and_set lock.state unlocked_state { pc = Locked [] }
    then ()
    else lock_blocking ~lock
  | { pc = Poisoned } -> raise Stopped
  | { pc = Locked _ } ->
    Thread.yield ();
    lock_blocking ~lock
;;

let with_lock_blocking ~f lock =
  lock_blocking ~lock;
  match apply_locked ~f ~lock with
  | exception exn ->
    poison_lock ~lock exn;
    raise Stopped
  | r ->
    unlock ~lock;
    r
;;

let rec try_lock ~f lock =
  match Atomic.get lock.state with
  | { pc = Unlocked } as unlocked_state ->
    if Atomic.compare_and_set lock.state unlocked_state { pc = Locked [] }
    then (
      match apply_locked ~f ~lock with
      | exception exn ->
        poison_lock ~lock exn;
        None
      | r ->
        unlock ~lock;
        Some r)
    else try_lock ~f lock
  | { pc = Poisoned | Locked _ } -> None
;;

let rec with_lock_deferred ~f lock =
  match Atomic.get lock.state with
  | { pc = Unlocked } as unlocked_state ->
    if Atomic.compare_and_set lock.state unlocked_state { pc = Locked [] }
    then (
      match apply_locked ~f ~lock with
      | exception exn -> poison_lock ~lock exn
      | () -> unlock ~lock)
    else with_lock_deferred ~f lock
  | { pc = Poisoned } -> ()
  | { pc = Locked defer_list } as locked_state ->
    if Atomic.compare_and_set lock.state locked_state { pc = Locked (f :: defer_list) }
    then ()
    else with_lock_deferred ~f lock
;;

let destroy ~f lock =
  lock_blocking ~lock;
  (* From here on, we have the lock's contents @ uncontended, and noone else will ever
     take the lock successfully again *)
  match apply_locked ~f ~lock with
  | exception exn ->
    poison_lock ~lock exn;
    raise Stopped
  | r ->
    poison_lock ~lock Stopped;
    r
;;

let contents_unlocked t = t.contents
