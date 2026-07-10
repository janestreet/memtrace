(* In non-OxCaml builds, this file has stub implementations of some OxCaml stdlib features *)

type 'a or_null =
  | Null
  | This of 'a

module Obj = struct
  include Obj

  let magic_uncontended = Fun.id
  let magic_portable = Fun.id
end

module Hashtbl = struct
  include Hashtbl
  module MakeSeededPortable = MakeSeeded
end

module Atomic = struct
  (* make_contended is available >= 5.2, so use make as a fallback *)
  let[@ocaml.warning "-32"] make_contended = Atomic.make

  include Atomic
end

module Domain = struct
  (* Fallback implementation, shadowed by the real one on 5.3+ *)
  let[@ocaml.warning "-32"] self_index () =
    (* Should be 'mod max_domains'. However, this is only used on <= 5.2, where
       max_domains is hardcoded as 256. *)
    (Domain.self () :> int) land 255
  ;;

  (* Remove this when [max_domain_count] is available upstream *)
  let[@ocaml.warning "-32"] max_domain_count = 1024

  let at_every_domain_exit ~f =
    (* This is not currently implementable with upstream's Domain support.

       (This is a pity, but the downside is missing a flush during domain termination,
       which will affect very few programs) *)
    ignore f
  ;;

  include Domain
end

module Gc = struct
  include Gc

  module Memprof = struct
    (* Stub these out until they are supported upstream *)
    let[@ocaml.warning "-32"] enlist _ = ()
    let[@ocaml.warning "-32"] enlist_all_domains _ = ()

    include Memprof
    module Safe = Memprof
  end
end

module Sys = struct
  let[@ocaml.warning "-32"] poll_actions () =
    (* Before poll_actions was added in 5.3, you could get a similar effect at slightly
       higher cost by doing a small allocation *)
    ignore (Sys.opaque_identity (ref 42))
  ;;

  include Sys
end
