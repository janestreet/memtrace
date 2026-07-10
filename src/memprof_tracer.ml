open Stdlib_shim

module Memprof_tracer_opt : sig @@ portable
  (* There's a nasty spot below where we need to initialise an optional field without
     allocating, because we've just turned on Memprof and want to avoid affecting the
     trace. Gc.Memprof.t or_null would suit, but when running with non-Oxcaml builds that
     is translated to something equivalent to option, and allocates. So instead, here's a
     hacky specialised or_null type for Gc.Memprof.t. *)
  type t : value mod contended portable

  val empty : t
  val of_memprof : Gc.Memprof.t -> t
  val is_empty : t -> bool
  val to_memprof_exn : t -> Gc.Memprof.t
end = struct
  type t = { contents : Obj.t @@ contended portable } [@@unboxed]

  let empty @ portable = Obj.magic_portable (Obj.magic "Memprof_tracer_opt.empty")
  let of_memprof t = Obj.magic t
  let is_empty t = t == Obj.magic_uncontended empty

  let to_memprof_exn t =
    if is_empty t then invalid_arg "Memprof_tracer_opt.empty" else Obj.magic t
  ;;
end

module Per_domain : sig @@ portable
  (** Domain-local storage.

      This module has a couple of advantages over Stdlib.Domain.DLS:
      - It's faster, since get and set are direct array references, involving no lazy
        initialisation
      - Get/set never allocate (important for memtrace, which is tracing allocations)
      - Get/set never invoke user callbacks (which makes locking harder to think about) *)
  type ('a : value_or_null mod contended portable separable)
       t :
       value mod contended portable

  val make : ('a : value_or_null mod contended portable separable). 'a -> 'a t
  val get : ('a : value_or_null mod contended portable separable). 'a t -> 'a
  val set : ('a : value_or_null mod contended portable separable). 'a t -> 'a -> unit

  (* Only atomic with respect to other systhreads on the same domain *)
  val compare_and_set
    : ('a : value_or_null mod contended portable separable).
    'a t -> 'a -> 'a -> bool

  (* Only atomic with respect to other systhreads on the same domain. Subtracts, and
     returns the value after subtraction. *)
  val sub_and_fetch : int t -> int -> int

  (* Only atomic with respect to other systhreads on the same domain *)
  val incr : int t -> unit

  (* Only atomic with respect to other systhreads on the same domain *)
  val decr : int t -> unit

  (* Racy accesses to slots other than the current domain's *)
  val get_for_domain
    : ('a : value_or_null mod contended portable separable).
    domain:int -> 'a t -> 'a

  val set_for_domain
    : ('a : value_or_null mod contended portable separable).
    domain:int -> 'a t -> 'a -> unit

  val iter_domains : f:(domain:int -> unit) -> unit
end = struct
  let stride =
    8 (* 8 * 8 byte words = 64 byte stride = cacheline on x86-64 and most arm64 chips *)
  ;;

  type ('a : value_or_null mod contended portable separable) t =
    { arr : 'a array @@ contended }
  [@@unboxed]

  let make x = { arr = Array.make (Domain.max_domain_count * stride) x }

  let get_for_domain ~domain t =
    Array.unsafe_get (Obj.magic_uncontended t.arr) (domain * stride)
  ;;

  let set_for_domain ~domain t x =
    Array.unsafe_set (Obj.magic_uncontended t.arr) (domain * stride) x
  ;;

  let get t = get_for_domain ~domain:(Domain.self_index ()) t
  let set t x = set_for_domain ~domain:(Domain.self_index ()) t x

  let iter_domains ~f =
    for domain = 0 to Domain.max_domain_count - 1 do
      f ~domain
    done
  ;;

  (* Below, the regions between BEGIN_ATOMIC/END_ATOMIC are atomic with respect to other
     systhreads on the same domain, by avoiding safepoints in the critical section. *)

  let[@inline never] compare_and_set t curr next =
    (* BEGIN ATOMIC *)
    let ix = Domain.self_index () * stride in
    let arr = Obj.magic_uncontended t.arr in
    if Array.unsafe_get arr ix == curr
    then (
      Array.unsafe_set arr ix next;
      true)
    else false
  ;;

  (* END ATOMIC *)

  let sub_and_fetch t k =
    let domain = Domain.self_index () in
    (* BEGIN ATOMIC *)
    let n = get_for_domain ~domain t in
    let n = n - k in
    set_for_domain ~domain t n;
    n
  ;;

  (* END ATOMIC *)

  let incr t =
    (* BEGIN ATOMIC *)
    set t (get t + 1)
  ;;

  (* END ATOMIC *)

  let decr t =
    (* BEGIN ATOMIC *)
    set t (get t - 1)
  ;;
  (* END ATOMIC *)
end

type active_tracer : value mod contended portable =
  { tracers : per_domain_tracer Lock.t or_null Per_domain.t
  ; report_exn : exn -> unit @@ portable
  ; memprof : Memprof_tracer_opt.t Atomic.t
  ; initial_ext_sampler : Geometric_sampler.t @@ contended
  ; initial_writer : Trace.Writer.t @@ contended
  ; no_new_tracers : bool Atomic.t
  ; ext_base_id : int
  }

and per_domain_tracer : value mod portable =
  { trace : Trace.Writer.t
  ; ext_sampler : Geometric_sampler.t
  ; part_of : active_tracer
  }

let equal_active_tracers t1 t2 =
  (* Since active_tracer is an immutable record, (==) isn't reliable. Instead, compare
     physical equality of the atomics holding the memprof state *)
  t1.memprof == t2.memprof
;;

type state =
  | Tracing of active_tracer
  | Halted of { ext_base_id : int }

let default_report_exn e =
  match e with
  | Trace.Writer.Pid_changed ->
    (* This error is silently ignored, so that if Memtrace is active across Unix.fork ()
       then the child process silently stops tracing *)
    ()
  | Lock.Stopped -> () (* This exception is expected during orderly shutdown *)
  | Trace.Writer.Closed | Buf.Shared_writer_fd.Closed ->
    () (* This happens if there is a race between stop () and writing an event *)
  | exn ->
    let msg = "Memtrace error: " ^ Printexc.to_string exn ^ "\n" in
    output_string stderr msg;
    Printexc.print_backtrace stderr;
    flush stderr
;;

let curr_active_tracer : state Atomic.t = Atomic.make (Halted { ext_base_id = 0 })

(* This lock provides mutual exclusion between different threads trying to *start* tracing
   at the same time.

   It is not used to *stop* tracing, which may occur at any time, including in the middle
   of a Gc.Memprof callback due to an error. So, curr_active_tracer can transition from
   Halted to Tracing only while holding this lock, but may transition from Tracing to
   Halted at any time. *)
let starting_lock : unit Lock.t =
  Lock.create ~on_error:(fun () exn -> default_report_exn exn) (fun () -> ())
;;

(* Number of bytes remaining to be allocated before the next Memtrace.External allocation. *)
let bytes_before_ext_sample = Per_domain.make 0

(* Nonzero [suspended] means that allocations should be ignored, because we're inside
   [ext_alloc_slowpath] *)
let suspended = Per_domain.make 0

let active_tracer () =
  match Atomic.get curr_active_tracer with
  | Tracing _ -> true
  | Halted _ -> false
;;

let current_domain () =
  let id = (Stdlib.Domain.self () :> int) in
  Trace.Domain_id.Expert.of_int id
;;

let draw_sampler_bytes t = Geometric_sampler.draw t.ext_sampler * (Sys.word_size / 8)

let stop_tracer tracer =
  let memprof = Atomic.get tracer.part_of.memprof in
  if not (Memprof_tracer_opt.is_empty memprof)
  then (
    let memprof = Memprof_tracer_opt.to_memprof_exn memprof in
    (* We can only stop a trace the current domain is participating in. We probably are
       already, but call enlist in case of a start/stop race. (In the normal case where
       we're already participating, it's a no-op that raises). *)
    (try Gc.Memprof.enlist memprof with
     | Failure _ -> () (* normal case *));
    (try Gc.Memprof.stop () with
     | Failure _ -> () (* already stopped *));
    (try Gc.Memprof.discard memprof with
     | Failure _ -> () (* runtime4 or already discarded *));
    Atomic.set tracer.part_of.memprof Memprof_tracer_opt.empty);
  Trace.Writer.close tracer.trace
;;

let set_halted ~old_state ~new_ext_base_id =
  let success =
    Atomic.compare_and_set
      curr_active_tracer
      old_state
      (Halted { ext_base_id = new_ext_base_id })
  in
  (* It's OK for that CAS to fail, since someone else might win the race to stop this
     trace *)
  ignore (success : bool)
;;

let (on_error @ portable) (tracer : per_domain_tracer) exn =
  (try tracer.part_of.report_exn exn with
   | exn' ->
     Printf.eprintf
       "Memtrace: report_exn failed when reporting %s: %s\n%!"
       (Printexc.to_string exn)
       (Printexc.to_string exn'));
  let count = stop_tracer tracer in
  match Atomic.get curr_active_tracer with
  | Tracing active as old_state when equal_active_tracers active tracer.part_of ->
    set_halted ~old_state ~new_ext_base_id:(active.ext_base_id + count)
  | _ ->
    (* Someone else already stopped this tracer *)
    ()
;;

let () =
  let do_flush s =
    try Lock.with_lock_blocking s ~f:(fun s -> Trace.Writer.flush s.trace) with
    | _ -> ()
  in
  let needs_flush s =
    try Lock.with_lock_blocking s ~f:(fun s -> Trace.Writer.needs_flush s.trace) with
    | _ -> false
  in
  Domain.at_every_domain_exit ~f:(fun () ->
    match Atomic.get curr_active_tracer with
    | Halted _ -> ()
    | Tracing active ->
      (match Per_domain.get active.tracers with
       | Null -> ()
       | This s ->
         while needs_flush s do
           do_flush s;
           (* It is possible that the above flush triggered a GC, causing more
              collect/promote events, requiring another flush *)
           Sys.poll_actions ()
         done))
;;

let final_flush (s : per_domain_tracer Lock.t) =
  try Lock.destroy s ~f:(fun s -> Trace.Writer.flush s.trace) with
  | Lock.Stopped | Trace.Writer.Closed -> ()
;;

let rec get_tracer (t : active_tracer) : per_domain_tracer Lock.t =
  match Per_domain.get t.tracers with
  | This s -> s
  | Null ->
    let s =
      Lock.create ~on_error (fun () ->
        let ext_sampler = Geometric_sampler.copy t.initial_ext_sampler in
        let trace =
          Trace.Writer.for_domain_at_time
            t.initial_writer
            ~start_time:(Trace.Timestamp.now ())
            ~domain:(current_domain ())
        in
        { ext_sampler; trace; part_of = t })
    in
    (* Avoid potential race with another systhread *)
    if Per_domain.compare_and_set t.tracers Null (This s)
    then (
      (* Read no_new_tracers *after* setting t.tracers, so either this or stop will do the
         final flush *)
      if Atomic.get t.no_new_tracers then final_flush s;
      s)
    else get_tracer t
;;

let tracker t : (_, _) Gc.Memprof.tracker =
  let allocate ~(info : Gc.Memprof.allocation) ~source : Trace.Obj_id.t option =
    if Per_domain.get suspended = 0
    then
      Lock.try_lock (get_tracer t) ~f:(fun s ->
        Trace.Writer.put_alloc_with_raw_backtrace
          s.trace
          (Trace.Timestamp.now ())
          ~length:info.size
          ~nsamples:info.n_samples
          ~source
          ~callstack:info.callstack)
      [@nontail]
    else None
  in
  let promote (id : Trace.Obj_id.t) : Trace.Obj_id.t option =
    Lock.with_lock_deferred (get_tracer t) ~f:(fun s ->
      Trace.Writer.put_promote s.trace (Trace.Timestamp.now ()) id);
    Some id
  in
  let dealloc (id : Trace.Obj_id.t) : unit =
    Lock.with_lock_deferred (get_tracer t) ~f:(fun s ->
      Trace.Writer.put_collect s.trace (Trace.Timestamp.now ()) id)
    [@nontail]
  in
  { alloc_minor = (fun info -> allocate ~info ~source:Minor)
  ; alloc_major = (fun info -> allocate ~info ~source:Major)
  ; promote
  ; dealloc_minor = dealloc
  ; dealloc_major = dealloc
  }
;;

let stop () =
  match Atomic.get curr_active_tracer with
  | Halted _ -> ()
  | Tracing ({ tracers; ext_base_id; _ } as t) as old_state ->
    (* Flush other domain's tracers before stopping the global trace *)
    Atomic.set t.no_new_tracers true;
    Per_domain.incr suspended;
    Per_domain.iter_domains ~f:(fun ~domain ->
      if domain <> Domain.self_index ()
      then (
        match Per_domain.get_for_domain ~domain tracers with
        | Null -> ()
        | This s -> final_flush s));
    Per_domain.decr suspended;
    let s = get_tracer t in
    (match Lock.destroy s ~f:stop_tracer with
     | exception (Lock.Stopped | Trace.Writer.Closed) -> ()
     | count -> set_halted ~old_state ~new_ext_base_id:(ext_base_id + count))
;;

let rec start
  ?(report_exn = default_report_exn)
  ~sampling_rate
  ~fd
  ?getpid
  ?write
  ~info
  ()
  =
  let res =
    Lock.with_lock_blocking starting_lock ~f:(fun () ->
      match Atomic.get curr_active_tracer with
      | Tracing _ -> `Stop_and_retry
      | Halted { ext_base_id } ->
        (try
           let s =
             Lock.create ~on_error (fun () ->
               let ext_sampler = Geometric_sampler.make ~sampling_rate () in
               let trace = Trace.Writer.create fd ?write ?getpid info in
               let memprof = Atomic.make Memprof_tracer_opt.empty in
               let tracers = Per_domain.make Null in
               let s =
                 { trace
                 ; ext_sampler
                 ; part_of =
                     { tracers
                     ; report_exn
                     ; memprof
                     ; initial_ext_sampler = ext_sampler
                     ; initial_writer = trace
                     ; no_new_tracers = Atomic.make false
                     ; ext_base_id
                     }
                 }
               in
               Per_domain.iter_domains ~f:(fun ~domain ->
                 Per_domain.set_for_domain
                   ~domain
                   bytes_before_ext_sample
                   (draw_sampler_bytes s));
               s)
           in
           let active_tracer = (Lock.contents_unlocked s).part_of in
           Per_domain.set active_tracer.tracers (This s);
           let new_state = Tracing active_tracer in
           let _ : _ = Sys.opaque_identity new_state in
           (* Ensure this allocation occurs before Gc.Memprof.start *)
           let tracker = tracker active_tracer in
           (* BEGIN ATOMIC *)
           let mp =
             Gc.Memprof.Safe.start ~sampling_rate ~callstack_size:max_int tracker
           in
           (* Initialize [active_tracer.memprof] before any sampled allocations. Doing
              this before the next safepoint ensures that no Memprof callback sees an
              uninitialized [active_tracer.memprof] *)
           Atomic.set active_tracer.memprof (Memprof_tracer_opt.of_memprof mp);
           (* END ATOMIC *)
           Atomic.set curr_active_tracer new_state;
           Gc.Memprof.enlist_all_domains mp;
           `Ok
         with
         | exn -> `Error exn))
  in
  match res with
  | `Stop_and_retry ->
    stop ();
    start ~report_exn ~sampling_rate ~fd ?getpid ?write ~info ()
  | `Ok -> ()
  | `Error exn -> raise exn
;;

let[@inline never] ext_alloc_slowpath ~(active : active_tracer) ~bytes =
  Per_domain.incr suspended;
  let res =
    try
      let s = get_tracer active in
      let bytes_per_word = Sys.word_size / 8 in
      (* round up to an integer number of words *)
      let size_words = (bytes + bytes_per_word - 1) / bytes_per_word in
      let callstack = Printexc.get_callstack max_int in
      let drop_slots =
        (* The last callstack slot will be exactly this function, since it's never
           inlined. We don't want to see it in the backtrace, so drop it here. *)
        1
      in
      let res =
        Lock.with_lock_blocking s ~f:(fun s ->
          let samples = ref 0 in
          let domain = Domain.self_index () in
          while
            let k = draw_sampler_bytes s in
            (* BEGIN ATOMIC *)
            let n = Per_domain.get_for_domain ~domain bytes_before_ext_sample in
            if n <= 0
            then (
              Per_domain.set_for_domain ~domain bytes_before_ext_sample (n + k);
              incr samples;
              n + k <= 0)
            else false
            (* END ATOMIC *)
          do
            ()
          done;
          if !samples = 0
          then (* Can happen when two systhreads race for the same sample *)
            Null
          else (
            let samples = !samples in
            This
              (Trace.Writer.put_alloc_with_suffix_of_raw_backtrace
                 s.trace
                 (Trace.Timestamp.now ())
                 ~length:size_words
                 ~nsamples:samples
                 ~source:External
                 ~callstack
                 ~drop_slots)))
      in
      match res with
      | This res -> This (active.ext_base_id + (res :> int))
      | Null -> Null
    with
    | _ -> Null
  in
  Per_domain.decr suspended;
  res
;;

type ext_token = int

let ext_alloc ~bytes =
  (* This function is about the most performance-sensitive one in this library. That's
     because it's called on *every* external allocation, not just those that Gc.Memprof
     has chosen to sample. So, it avoids taking any locks, and instead uses mutable int
     fields while avoiding poll points. *)
  match Atomic.get curr_active_tracer with
  | Halted _ -> Null
  | Tracing active ->
    let n = Per_domain.sub_and_fetch bytes_before_ext_sample bytes in
    if n <= 0
    then
      (* This has [@tail] to make sure this function won't appear in any backtraces
         (unless it's inlined into another function, in which case we have to filter it
         out after the fact). *)
      ext_alloc_slowpath ~active ~bytes [@tail]
    else Null
;;

let ext_free id =
  (* This is not as perf-sensitive as ext_alloc, as it's only called on traced blocks *)
  match Atomic.get curr_active_tracer with
  | Halted _ -> ()
  | Tracing active ->
    if id < active.ext_base_id
    then (* This is an old ID, from a prior Memtrace incarnation. Ignore it. *)
      ()
    else (
      let id = Trace.Obj_id.Expert.of_int (id - active.ext_base_id) in
      Per_domain.incr suspended;
      (try
         Lock.with_lock_deferred (get_tracer active) ~f:(fun s ->
           Trace.Writer.put_collect s.trace (Trace.Timestamp.now ()) id)
       with
       | _ -> ());
      Per_domain.decr suspended)
;;

let () = Trace.Private.set_name_of_memprof_tracer_module __MODULE__
