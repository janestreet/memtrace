@@ portable

(** An 'a t is a spinlock with some extra states: as well as being locked or unlocked, it
    may also be:

    - Poisoned: Locking always fails in this state, which is reached by failing with an
      exception while holding the lock

    - Deferred (represented as Locked with an nonempty list): While the lock was held,
      extra work was added asynchronously, to be performed before the lock is unlocked. *)
type ('a : value mod portable) t : value mod contended non_float portable

val create
  :  on_error:('a -> exn -> unit) @ portable
  -> (unit -> 'a) @ local once portable unyielding
  -> 'a t

exception Stopped

(** Run [f] while holding the lock. If the lock is held, spin calling Thread.yield ()
    until it is released. If the lock is poisoned, raises [Stopped]. If [f] raises, then
    its exception is re-raised and the lock becomes poisoned. *)
val with_lock_blocking
  : ('a : value mod portable) ('r : value_or_null).
  f:('a -> 'r @ portable) @ local once portable unyielding
  -> 'a t
  -> 'r @ contended portable

(** Try to run [f] while holding the lock. If the lock is held or poisoned, return
    immediately without doing anything. *)
val try_lock
  :  f:('a -> 'r @ portable) @ local once portable unyielding
  -> 'a t
  -> 'r option @ contended portable

(** Run [f] while holding the lock, now or later. If the lock is held, return immediately,
    deferring the call to [f] until the lock becomes available. If the lock is poisoned,
    do nothing.

    This function can safely be used from within an asynchronous context (e.g. signal
    handler or memprof callback), as it will not block if the lock is held by the current
    thread. *)
val with_lock_deferred : f:('a -> unit @ portable) @ portable unyielding -> 'a t -> unit

(** Same as [with_lock_blocking], except that the lock is left poisoned afterwards *)
val destroy
  : ('a : value mod portable) ('r : value_or_null).
  f:('a -> 'r @ portable) @ local once portable unyielding
  -> 'a t
  -> 'r @ contended portable

(** Returns the value protected by the lock, _without locking_.

    The value is returned @ contended, so this is only useful for accessing immutable
    parts of it *)
val contents_unlocked : 'a t -> 'a @ contended
