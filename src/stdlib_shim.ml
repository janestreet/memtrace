(* In OxCaml builds, this file does almost nothing.

   In upstream builds, it is replaced with stdlib_shim_upstream.ml, to add stub
   implementations of some OxCaml stdlib features that memtrace uses. *)

(* Hack to avoid "unused open" warnings. *)
type nonrec unit = unit

external ( == )
  : ('a : value_or_null).
  ('a[@local_opt]) @ contended -> ('a[@local_opt]) @ contended -> bool
  @@ portable
  = "%eq"

module Atomic = struct
  include Atomic

  (* Atomic.make_contended is provided but broken on runtime4 builds of oxcaml *)
  external runtime5 : unit -> bool @@ portable = "%runtime5"

  let make_contended = if runtime5 () then make_contended else make
end

module Domain = struct
  let[@ocaml.warning "-32"] max_domain_count = 1024

  (* Arranges for [f] to be called at exit of every domain spawned after the call to
     [at_every_domain_exit f] *)
  let at_every_domain_exit ~f =
    let _key : unit Domain.DLS.key =
      Domain.Safe.DLS.new_key
        (fun () -> ())
        ~split_from_parent:(fun () () -> Domain.Safe.at_exit f)
    in
    ()
  ;;

  include Domain
end
