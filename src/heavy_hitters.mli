module Make(A : Hashtbl.HashedType) : sig
  type t
  val make : int -> t
  val add : t -> A.t -> unit
  val length : t -> int
  val iter : t -> (A.t -> int -> int -> unit) -> unit
end
