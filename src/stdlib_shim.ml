(* In OxCaml builds, this file does nothing.

   In upstream builds, it is replaced with stdlib_shim_upstream.ml, to add stub
   implementations of some OxCaml stdlib features that memtrace uses. *)

(* Hack to avoid "unused open" warnings. *)
type nonrec unit = unit
