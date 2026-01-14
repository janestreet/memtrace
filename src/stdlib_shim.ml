(* In non-OxCaml builds, this file has stub implementations of some OxCaml stdlib features *)

type 'a or_null =
  | Null
  | This of 'a

module Obj = struct
  include Obj

  let magic_uncontended = Fun.id
end

module Hashtbl = struct
  include Hashtbl
  module MakeSeededPortable = MakeSeeded
end
