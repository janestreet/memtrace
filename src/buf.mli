type _ buf = private {
  buf : Bytes.t;
  mutable pos : int;
  pos_end : int;
}

type rbuf = [`R] buf
type wbuf = [`W] buf
val of_bytes : Bytes.t -> _ buf
val of_bytes_sub : Bytes.t -> pos:int -> pos_end:int -> _ buf
val empty : rbuf
val remaining : _ buf -> int

val split_buf : 'a buf -> int -> ('a buf * 'a buf)

val read_fd : Unix.file_descr -> Bytes.t -> rbuf
val refill_fd : Unix.file_descr -> rbuf -> rbuf
val write_fd : Unix.file_descr -> wbuf -> unit

exception Underflow of int
val get_8  : rbuf -> int
val get_16 : rbuf -> int
val get_32 : rbuf -> int32
val get_64 : rbuf -> int64
val get_float : rbuf -> float
val get_string : rbuf -> string
val get_vint : rbuf -> int (* NB: overflow *)

exception Overflow of int
val put_8  : wbuf -> int -> unit
val put_16 : wbuf -> int -> unit
val put_32 : wbuf -> int32 -> unit
val put_64 : wbuf -> int64 -> unit
val put_float : wbuf -> float -> unit
val put_string : wbuf -> string -> unit
val put_vint : wbuf -> int -> unit

type _ position = private int
val skip_8  : wbuf -> [`Int8] position
val skip_16 : wbuf -> [`Int16] position
val skip_32 : wbuf -> [`Int32] position
val skip_64 : wbuf -> [`Int64] position
val skip_float : wbuf -> [`Float] position

val update_8  : wbuf -> [`Int8] position -> int -> unit
val update_16 : wbuf -> [`Int16] position -> int -> unit
val update_32 : wbuf -> [`Int32] position -> int32 -> unit
val update_64 : wbuf -> [`Int64] position -> int64 -> unit
val update_float : wbuf -> [`Float] position -> float -> unit
