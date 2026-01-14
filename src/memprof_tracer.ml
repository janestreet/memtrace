open Stdlib_shim

(* The tracer requires some tricky locking.

   It can be used both synchronously from user code (e.g. in calls to ext_alloc)
   and asynchronously from Gc.Memprof callbacks. Locks in async callbacks cannot
   block, as they might interrupt code which synchronously holds the lock, and
   so would deadlock if they blocked.

   Instead, users of lock_async must handle the case where the lock is already held
   synchronously by the current thread. In this case, the lock can't be taken, and
   instead the user must defer some work to be processed when the synchronous holder
   later releases the lock.

   In other words, the lock is nonreentrant: you can't take a lock you already hold.

   This module is implemented with nonatomic references, and should only be used on
   the main domain (that is, when [Domain.is_main_domain ()]). *)
module Lock = struct
  type 'a t =
    { mutable locked : bool
    ; mutable locked_sync : bool
    ; mutable locked_sync_thread_id : int
    ; mutable stopped : bool
    ; mutable deferred : 'a list
    }

  let create () =
    { locked = false
    ; locked_sync = false
    ; locked_sync_thread_id = -1
    ; stopped = false
    ; deferred = []
    }
  ;;

  let self_id () = Thread.id (Thread.self ())

  (* lock_sync and unlock_sync form a spinlock with two extra features:
     - Work may be 'deferred' (see defer and lock_async below), to run at unlock time
     - There is a 'stopped' state (see unlock_and_stop), in which locking always fails *)

  type lock_sync_result =
    | Success
    | Is_stopped

  let[@inline never] rec lock_sync s : lock_sync_result =
    if s.stopped
    then Is_stopped
    else if s.locked
    then (
      if s.locked_sync && s.locked_sync_thread_id = self_id ()
      then failwith "Memprof_tracer.Lock.lock_sync: Attempted to lock recursively";
      Thread.yield ();
      lock_sync s)
    else (
      s.locked <- true;
      s.locked_sync <- true;
      s.locked_sync_thread_id <- self_id ();
      Success)
  ;;

  let[@inline never] defer s t =
    assert (
      (not s.stopped) && s.locked && s.locked_sync && s.locked_sync_thread_id = self_id ());
    s.deferred <- t :: s.deferred
  ;;

  let[@inline never] unlock_and_stop s =
    s.stopped <- true;
    s.deferred <- [];
    s.locked <- false;
    s.locked_sync <- false
  ;;

  let[@inline never] rec unlock_sync ~report_exn ~handle_deferred s =
    assert (
      (not s.stopped) && s.locked && s.locked_sync && s.locked_sync_thread_id = self_id ());
    match s.deferred with
    | [] ->
      s.locked_sync <- false;
      s.locked_sync_thread_id <- -1;
      s.locked <- false
    | deferred ->
      s.deferred <- [];
      (match List.iter handle_deferred (List.rev deferred) with
       | () -> unlock_sync ~report_exn ~handle_deferred s
       | exception e ->
         unlock_and_stop s;
         report_exn e)
  ;;

  (* Normally, taking a lock during an asynchronous callback can cause deadlocks, as the
     lock may already be held by the thread that is currently running the asynchronous
     handler. So, it is incorrect to use [lock_sync] / [unlock_sync] from a handler.

     Instead, [lock_async] and [unlock_async] may be used from asynchronous
     handlers. Taking a lock asynchronously can return a [Is_sync_locked_by_this_thread],
     indicating that the lock is already held by this thread and so cannot be waited for.

     In this state, it is not in general safe to manipulate the state protected by the lock
     (since you don't know what the interrupted thread is doing with it), but you can use
     [defer] to schedule work for when the interrupted thread releases the lock. *)

  type lock_async_result =
    | Success
    | Is_sync_locked_by_this_thread
    | Is_stopped

  let[@inline never] rec lock_async s : lock_async_result =
    if s.stopped
    then Is_stopped
    else if s.locked_sync && s.locked_sync_thread_id = self_id ()
    then Is_sync_locked_by_this_thread
    else if s.locked
    then (
      Thread.yield ();
      lock_async s)
    else (
      s.locked <- true;
      Success)
  ;;

  let[@inline never] unlock_async s =
    assert ((not s.stopped) && s.locked && not s.locked_sync);
    s.locked <- false
  ;;
end

type deferred_event =
  | Deferred_promote of Trace.Writer.t * Trace.Obj_id.t
  | Deferred_collect of Trace.Writer.t * Trace.Obj_id.t

let handle_deferred_event = function
  | Deferred_promote (t, id) -> Trace.Writer.put_promote t (Trace.Timestamp.now ()) id
  | Deferred_collect (t, id) -> Trace.Writer.put_collect t (Trace.Timestamp.now ()) id
;;

type t =
  { lock : deferred_event Lock.t
  ; report_exn : exn -> unit
  ; trace : Trace.Writer.t
  ; ext_sampler : Geometric_sampler.t
  }

let curr_active_tracer : t option ref = ref None
let active_tracer () = !curr_active_tracer

let current_domain () =
  let id = (Stdlib.Domain.self () :> int) in
  Trace.Domain_id.Expert.of_int id
;;

let bytes_before_ext_sample = ref max_int
let draw_sampler_bytes t = Geometric_sampler.draw t.ext_sampler * (Sys.word_size / 8)

let[@inline never] mark_failed s e =
  Lock.unlock_and_stop s.lock;
  s.report_exn e
;;

let default_report_exn e =
  match e with
  | Trace.Writer.Pid_changed ->
    (* This error is silently ignored, so that if Memtrace is active across
        Unix.fork () then the child process silently stops tracing *)
    ()
  | e ->
    let msg = Printf.sprintf "Memtrace failure: %s\n" (Printexc.to_string e) in
    output_string stderr msg;
    Printexc.print_backtrace stderr;
    flush stderr
;;

let assert_main_domain () =
  if not (Domain.is_main_domain ())
  then failwith "Memtrace can currently only be used from the main domain"
;;

let start ?(report_exn = default_report_exn) ~sampling_rate trace =
  assert_main_domain ();
  let ext_sampler = Geometric_sampler.make ~sampling_rate () in
  let s = { trace; lock = Lock.create (); report_exn; ext_sampler } in
  let allocate ~(info : Gc.Memprof.allocation) ~source : Trace.Obj_id.t option =
    if not (Domain.is_main_domain ())
    then None
    else (
      match Lock.lock_async s.lock with
      | Is_stopped -> None
      | Is_sync_locked_by_this_thread ->
        None (* Ignore allocations during e.g. ext_alloc *)
      | Success ->
        (match
           Trace.Writer.put_alloc_with_raw_backtrace
             trace
             (Trace.Timestamp.now ())
             ~length:info.size
             ~nsamples:info.n_samples
             ~source
             ~callstack:info.callstack
         with
         | r ->
           Lock.unlock_async s.lock;
           Some r
         | exception e ->
           mark_failed s e;
           None))
  in
  let promote id : Trace.Obj_id.t option =
    if not (Domain.is_main_domain ())
    then None
    else (
      match Lock.lock_async s.lock with
      | Is_stopped -> None
      | Is_sync_locked_by_this_thread ->
        Lock.defer s.lock (Deferred_promote (trace, id));
        Some id
      | Success ->
        (match Trace.Writer.put_promote trace (Trace.Timestamp.now ()) id with
         | () ->
           Lock.unlock_async s.lock;
           Some id
         | exception e ->
           mark_failed s e;
           None))
  in
  let dealloc id =
    if not (Domain.is_main_domain ())
    then ()
    else (
      match Lock.lock_async s.lock with
      | Is_stopped -> ()
      | Is_sync_locked_by_this_thread -> Lock.defer s.lock (Deferred_collect (trace, id))
      | Success ->
        (match Trace.Writer.put_collect trace (Trace.Timestamp.now ()) id with
         | () -> Lock.unlock_async s.lock
         | exception e -> mark_failed s e))
  in
  let tracker : (_, _) Gc.Memprof.tracker =
    { alloc_minor = (fun info -> allocate ~info ~source:Minor)
    ; alloc_major = (fun info -> allocate ~info ~source:Major)
    ; promote
    ; dealloc_minor = dealloc
    ; dealloc_major = dealloc
    }
  in
  (* Pre-allocate these so that they don't get included in the trace (and use
     [Sys.opaque_identity] so the allocations don't get moved *)
  let active_tracer, sampler_bytes = Sys.opaque_identity (Some s, draw_sampler_bytes s) in
  ignore (Gc.Memprof.start ~sampling_rate ~callstack_size:max_int tracker : Gc.Memprof.t);
  curr_active_tracer := active_tracer;
  bytes_before_ext_sample := sampler_bytes;
  s
;;

let stop s =
  assert_main_domain ();
  match Lock.lock_sync s.lock with
  | Is_stopped -> ()
  | Success ->
    Lock.unlock_and_stop s.lock;
    (match !curr_active_tracer with
     | None -> ()
     | Some _ ->
       Gc.Memprof.stop ();
       (try Trace.Writer.close s.trace with
        | e -> s.report_exn e);
       curr_active_tracer := None)
;;

let[@inline never] ext_alloc_slowpath ~bytes : Trace.Obj_id.t or_null =
  match !curr_active_tracer with
  | None ->
    bytes_before_ext_sample := max_int;
    Null
  | Some s ->
    (match Lock.lock_sync s.lock with
     | Is_stopped -> Null
     | Success ->
       (match
          let bytes_per_word = Sys.word_size / 8 in
          (* round up to an integer number of words *)
          let size_words = (bytes + bytes_per_word - 1) / bytes_per_word in
          let samples = ref 0 in
          while !bytes_before_ext_sample <= 0 do
            bytes_before_ext_sample := !bytes_before_ext_sample + draw_sampler_bytes s;
            incr samples
          done;
          assert (!samples > 0);
          let callstack = Printexc.get_callstack max_int in
          let drop_slots =
            (* The last callstack slot will be exactly this function, since it's
             never inlined. We don't want to see it in the backtrace, so drop it
             here. *)
            1
          in
          (* Sys.opaque_identity ensures that flambda2 doesn't move the
           allocation past the [unlock_tracer_ext] call *)
          This
            (Trace.Writer.put_alloc_with_suffix_of_raw_backtrace
               s.trace
               (Trace.Timestamp.now ())
               ~length:size_words
               ~nsamples:!samples
               ~source:External
               ~callstack
               ~drop_slots)
          |> Sys.opaque_identity
        with
        | r ->
          Lock.unlock_sync
            ~report_exn:s.report_exn
            ~handle_deferred:handle_deferred_event
            s.lock;
          r
        | exception e ->
          mark_failed s e;
          Null))
;;

type ext_token = Trace.Obj_id.t

let ext_alloc ~bytes =
  let next_sample = !bytes_before_ext_sample in
  if next_sample = max_int
  then Null
  else if not (Domain.is_main_domain ())
  then Null
  else (
    let n = next_sample - bytes in
    bytes_before_ext_sample := n;
    if n <= 0
    then
      (* This has [@tail] to make sure this function won't appear in any backtraces (unless
         it's inlined into another function, in which case we have to filter it out after
         the fact). *)
      ext_alloc_slowpath ~bytes [@tail]
    else Null)
;;

let ext_free id =
  match !curr_active_tracer with
  | None -> ()
  | Some s ->
    if Domain.is_main_domain ()
    then (
      match Lock.lock_sync s.lock with
      | Is_stopped -> ()
      | Success ->
        (match Trace.Writer.put_collect s.trace (Trace.Timestamp.now ()) id with
         | () ->
           Lock.unlock_sync
             ~report_exn:s.report_exn
             ~handle_deferred:handle_deferred_event
             s.lock;
           ()
         | exception e ->
           mark_failed s e;
           ()))
;;

let () = Trace.Private.set_name_of_memprof_tracer_module __MODULE__
