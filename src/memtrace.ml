module Deps = struct
  type out_file = Unix.file_descr
  let open_out filename =
    Unix.openfile filename Unix.[O_CREAT;O_WRONLY;O_TRUNC] 0o666
  let write fd b off len = Unix.write fd b off len
  let close_out fd = Unix.close fd

  type in_file = Unix.file_descr
  let open_in filename =
    Unix.openfile filename [Unix.O_RDONLY] 0
  let read fd b off len = Unix.read fd b off len
  let close_in fd = Unix.close fd

  let timestamp () = Unix.gettimeofday ()

  type allocation = Gc.Memprof.allocation = private
    { n_samples : int;
      size : int;
      unmarshalled : bool;
      callstack : Printexc.raw_backtrace
    }

  let memprof_start
    ~callstack_size
    ~(minor_alloc_callback:allocation -> _)
    ~(major_alloc_callback:allocation -> _)
    ~promote_callback
    ~minor_dealloc_callback
    ~major_dealloc_callback
    ~sampling_rate
    () : unit =
(*
    ignore (callstack_size, minor_alloc_callback, major_alloc_callback,
            promote_callback, minor_dealloc_callback, major_dealloc_callback, sampling_rate);
    assert false
*)
    Gc.Memprof.start ~callstack_size ~minor_alloc_callback ~major_alloc_callback
      ~promote_callback ~minor_dealloc_callback ~major_dealloc_callback ~sampling_rate
      ()
  let memprof_stop () : unit = Gc.Memprof.stop ()

end

(* Increment this when the format changes *)
let memtrace_version = 1
let cache_enable_debug = false

module IntTbl = Hashtbl.MakeSeeded (struct
  type t = int
  let hash _seed (id : t) =
    let h = id * 189696287 in
    h lxor (h lsr 23)
  let equal (a : t) (b : t) = a = b
end)

(* Buffer management *)

type buffer = {
  buf : Bytes.t;
  pos_end : int;
  mutable pos : int;
}

let remaining b =
  b.pos_end - b.pos

let mkbuffer buf off len =
  if len < 0 then
    raise (Invalid_argument "mkbuffer: negative length");
  if off + len > Bytes.length buf then
    raise (Invalid_argument "mkbuffer: out of bounds");
  { buf; pos = off; pos_end = off + len }

let mkbuffer buf =
  mkbuffer buf 0 (Bytes.length buf)

let put_raw_8 b i v = Bytes.unsafe_set b i (Char.unsafe_chr v)
external put_raw_16 : Bytes.t -> int -> int -> unit = "%caml_bytes_set16u"
external put_raw_32 : Bytes.t -> int -> int32 -> unit = "%caml_bytes_set32u"
external put_raw_64 : Bytes.t -> int -> int64 -> unit = "%caml_bytes_set64u"
external get_raw_16 : Bytes.t -> int -> int = "%caml_bytes_get16u"
external get_raw_32 : Bytes.t -> int -> int32 = "%caml_bytes_get32u"
external get_raw_64 : Bytes.t -> int -> int64 = "%caml_bytes_get64u"
external bswap_16 : int -> int = "%bswap16"
external bswap_32 : int32 -> int32 = "%bswap_int32"
external bswap_64 : int64 -> int64 = "%bswap_int64"

exception Gen_error of [`Overflow of int]
exception Parse_error of [`Underflow of int | `Bad_format of string]
let () =
  Printexc.register_printer (function
    | Parse_error (`Bad_format s) ->
      Some ("malformed trace: " ^ s)
    | Parse_error (`Underflow s) ->
      Some ("malformed trace: underflow at pos " ^ string_of_int s)
    | _ -> None)

let[@inline never] overflow b = raise (Gen_error (`Overflow b.pos))
let[@inline never] underflow b = Parse_error (`Underflow b.pos)
let[@inline never] bad_format s = raise (Parse_error (`Bad_format s))
let[@inline never] bad_formatf f = Printf.ksprintf (fun s -> bad_format s) f
let check_fmt s b = if not b then bad_format s


let put_8 b v =
  let pos = b.pos in
  let pos' = b.pos + 1 in
  if pos' > b.pos_end then overflow b else
  (put_raw_8 b.buf pos v;
   b.pos <- pos')
let put_16 b v =
  let pos = b.pos in
  let pos' = b.pos + 2 in
  if pos' > b.pos_end then overflow b else
  (put_raw_16 b.buf pos (if Sys.big_endian then bswap_16 v else v);
   b.pos <- pos')
let put_32 b v =
  let pos = b.pos in
  let pos' = b.pos + 4 in
  if pos' > b.pos_end then overflow b else
  (put_raw_32 b.buf pos (if Sys.big_endian then bswap_32 v else v);
   b.pos <- pos')
let put_64 b v =
  let pos = b.pos in
  let pos' = b.pos + 8 in
  if pos' > b.pos_end then overflow b else
  (put_raw_64 b.buf pos (if Sys.big_endian then bswap_64 v else v);
   b.pos <- pos')
let[@inline never] put_vint_big b v =
  if v = v land 0xffff then
    (put_8 b 253; put_16 b v)
  else if v = v land 0xffffffff then
    (put_8 b 254; put_32 b (Int32.of_int v))
  else
    (put_8 b 255; put_64 b (Int64.of_int v))
let put_vint b v =
  if 0 <= v && v <= 252 then
    put_8 b v
  else
    put_vint_big b v
let put_string b s =
  let slen = String.length s in
  if b.pos + slen + 1 > b.pos_end then overflow b;
  Bytes.blit_string s 0 b.buf b.pos slen;
  Bytes.unsafe_set b.buf (b.pos + slen) '\000';
  b.pos <- b.pos + slen + 1
let put_float b f =
  put_64 b (Int64.bits_of_float f)

let[@inline always] get_8 b =
  let pos = b.pos in
  let pos' = b.pos + 1 in
  if pos' > b.pos_end then raise (underflow b);
  b.pos <- pos';
  Char.code (Bytes.unsafe_get b.buf pos)
let[@inline always] get_16 b =
  let pos = b.pos in
  let pos' = b.pos + 2 in
  if pos' > b.pos_end then raise (underflow b);
  b.pos <- pos';
  if Sys.big_endian then bswap_16 (get_raw_16 b.buf pos) else get_raw_16 b.buf pos
let[@inline always] get_32 b =
  let pos = b.pos in
  let pos' = b.pos + 4 in
  if pos' > b.pos_end then raise (underflow b);
  b.pos <- pos';
  if Sys.big_endian then bswap_32 (get_raw_32 b.buf pos) else get_raw_32 b.buf pos
let[@inline always] get_64 b =
  let pos = b.pos in
  let pos' = b.pos + 8 in
  if pos' > b.pos_end then raise (underflow b);
  b.pos <- pos';
  if Sys.big_endian then bswap_64 (get_raw_64 b.buf pos) else get_raw_64 b.buf pos
(* FIXME: overflow if deserialised on 32-bit. Should I care? *)
let[@inline always] get_vint b =
  match get_8 b with
  | 253 -> get_16 b
  | 254 -> Int32.to_int (get_32 b)
  | 255 -> Int64.to_int (get_64 b)
  | n -> n
let get_string b =
  let start = b.pos in
  while get_8 b <> 0 do () done;
  let len = b.pos - 1 - start in
  Bytes.sub_string b.buf start len
let get_float b =
  Int64.float_of_bits (get_64 b)



type times = { mutable t_start : float; mutable t_end : float }

let cache_size = 1 lsl 14
type cache_bucket = int  (* 0 to cache_size - 1 *)

type reader_cache = {
  cache_loc : int array;
  cache_pred : int array;
  mutable last_backtrace : int array;
}

let create_reader_cache () =
  { cache_loc = Array.make cache_size 0;
    cache_pred = Array.make cache_size 0;
    last_backtrace = [| |] }

type writer_cache = {
  cache : int array;
  (* when an entry was added to the cache (used for eviction) *)
  cache_date : int array;
  (* last time we saw this entry, which entry followed it? *)
  cache_next : cache_bucket array;
  (* for debugging *)
  debug_cache : reader_cache option;
  mutable next_verify_ix : int;
}

let create_writer_cache () =
  { cache = Array.make cache_size 0;
    cache_date = Array.make cache_size 0;
    cache_next = Array.make cache_size 0;
    debug_cache = if cache_enable_debug then Some (create_reader_cache ()) else None;
    next_verify_ix = 0 }

type mtf_table = string array

type ctf_header_offsets =
  { hdr_buf : Bytes.t;
    off_packet_size : int;
    off_timestamp_begin : int;
    off_timestamp_end : int;
    off_flush_duration : int;
    off_alloc_begin : int;
    off_alloc_end : int }

type tracer = {
  dest : Deps.out_file;
  file_mtf : mtf_table;
  defn_mtfs : mtf_table array;
  mutable new_locs : (int * Printexc.raw_backtrace_slot) array;
  mutable new_locs_len : int;
  new_locs_buf : Bytes.t;
  mutable last_callstack : int array;

  cache : writer_cache;

  mutable start_alloc_id : int; (* alloc ID at start of packet *)
  mutable next_alloc_id : int;
  mutable packet_times : times;
  mutable packet_header : ctf_header_offsets;
  mutable packet : buffer;

  mutable stopped : bool;

  mutable locked : bool;
}

let[@inline never] lock_tracer s =
  (* This is a maximally unfair spinlock. *)
  (* FIXME: correctness rests on dubious assumptions of atomicity *)
  (* if s.locked then Printf.fprintf stderr "contention\n%!"; *)
  while s.locked do Thread.yield () done;
  s.locked <- true

let[@inline never] unlock_tracer s =
  assert (s.locked);
  s.locked <- false

let log_new_loc s loc =
  let alen = Array.length s.new_locs in
  assert (s.new_locs_len <= alen);
  if s.new_locs_len = alen then begin
    let new_len = if alen = 0 then 32 else alen * 2 in
    let locs = Array.make new_len loc in
    Array.blit s.new_locs 0 locs 0 alen;
    s.new_locs <- locs;
    s.new_locs_len <- alen + 1
  end else begin
    s.new_locs.(s.new_locs_len) <- loc;
    s.new_locs_len <- s.new_locs_len + 1
  end

let to_timestamp_64 t =
  t *. 1_000_000. |> Float.to_int |> Int64.of_int

type packet_header_info = {
  content_size: int; (* bytes, excluding header *)
  time_begin : Int64.t;
  time_end : Int64.t;
  alloc_id_begin : Int64.t;
  alloc_id_end : Int64.t;
  cache_verify_ix : int;
  cache_verify_pred : int;
  cache_verify_val : Int64.t;
  pid : Int64.t
}

type evcode = Ev_trace_info | Ev_location | Ev_alloc | Ev_promote | Ev_collect | Ev_short_alloc of int
let event_code = function
  | Ev_trace_info -> 0
  | Ev_location -> 1
  | Ev_alloc -> 2
  | Ev_promote -> 3
  | Ev_collect -> 4
  | Ev_short_alloc n ->
     assert (1 <= n && n <= 16);
     100 + n
let event_of_code = function
  | 0 -> Ev_trace_info
  | 1 -> Ev_location
  | 2 -> Ev_alloc
  | 3 -> Ev_promote
  | 4 -> Ev_collect
  | n when 101 <= n && n <= 116 ->
     Ev_short_alloc (n - 100)
  | c -> bad_format ("Unknown event code " ^ string_of_int c)

let event_header_time_len = 25
let event_header_time_mask = 0x1ffffffl
let put_event_header b ev t =
  let t = to_timestamp_64 t in
  let code =
    Int32.(logor (shift_left (of_int (event_code ev))
                    event_header_time_len)
             (logand (Int64.to_int32 t) event_header_time_mask)) in
  put_32 b code
let[@inline] get_event_header info b =
  let code = get_32 b in
  let start_low = Int32.logand event_header_time_mask (Int64.to_int32 info.time_begin) in
  let time_low = Int32.logand event_header_time_mask code in
  let time_low =
    if time_low < start_low then
      (* Overflow *)
      Int32.(add time_low (of_int (1 lsl event_header_time_len)))
    else
      time_low in
  let time =
    Int64.(add (logand info.time_begin (lognot (of_int32 event_header_time_mask)))
             (of_int32 time_low)) in
  check_fmt "time in packet bounds" (info.time_begin <= time && time <= info.time_end);
  let ev = event_of_code (Int32.(to_int (shift_right_logical code
                                           event_header_time_len))) in
  (ev, time)

let mtf_length = 31
let create_mtf_table () =
  Array.init mtf_length (fun i ->
    (* entries in MTF table must be distinct *)
    ("??" ^ string_of_int i))
let mtf_search mtf filename =
  let rec insert mtf prev filename i =
    if i = mtf_length then
      (* not found *)
      i
    else begin
      let curr = mtf.(i) in
      mtf.(i) <- prev;
      if String.equal curr filename then
        i
      else
        insert mtf curr filename (i+1)
    end in
  if String.equal mtf.(0) filename then
    0
  else
    let prev = mtf.(0) in
    mtf.(0) <- filename;
    insert mtf prev filename 1
let mtf_swap mtf i =
  assert (i < mtf_length);
  if i <> 0 then begin
    let f = mtf.(i) in
    Array.blit mtf 0 mtf 1 i;
    mtf.(0) <- f
  end
let mtf_bump mtf =
  Array.blit mtf 0 mtf 1 (mtf_length - 1)

type location = {
  filename : string;
  line : int;
  start_char : int;
  end_char : int;
  defname : string;
}

(* FIXME: max_location overflow *)
let max_location = 4 * 1024
let put_backtrace_slot b file_mtf defn_mtfs (id, loc) =
  let open Printexc in
  let rec get_locations slot =
    let tail =
      match get_raw_backtrace_next_slot slot with
      | None -> []
      | Some slot -> get_locations slot in
    let slot = convert_raw_backtrace_slot slot in
    match Slot.location slot, Slot.name slot with
    | None, _ -> tail
    | Some l, None -> (l, "??") :: tail
    | Some l, Some d -> (l,d) :: tail in
  let locs = get_locations loc |> List.rev in
  let max_locs = 255 in
  let locs =
    if List.length locs <= max_locs then locs else
      ((*(List.filteri (fun i _ -> i < max_locs - 1) locs)
        @*)
      [ { filename = "<unknown>"; line_number = 1; start_char = 1; end_char = 1 }, "??" ]) in
  assert (List.length locs <= max_locs);
  put_64 b (Int64.of_int id);
  put_8 b (List.length locs);
  locs |> List.iter (fun ((loc : location), defname) ->
    let clamp n lim = if n < 0 || n > lim then lim else n in
    let line_number = clamp loc.line_number 0xfffff in
    let start_char = clamp loc.start_char 0xfff in
    let end_char = clamp loc.end_char 0xfff in
    let filename_code = mtf_search file_mtf loc.filename in
    mtf_swap defn_mtfs (if filename_code = mtf_length then
                          mtf_length - 1 else filename_code);
    let defname_code = mtf_search defn_mtfs.(0) defname in
    let encoded =
      Int64.(
        logor (of_int line_number)
       (logor (shift_left (of_int start_char) 20)
       (logor (shift_left (of_int end_char) (20 + 8))
       (logor (shift_left (of_int filename_code) (20 + 8 + 10))
              (shift_left (of_int defname_code) (20 + 8 + 10 + 5)))))) in
    put_32 b (Int64.to_int32 encoded);
    put_16 b (Int64.(to_int (shift_right encoded 32)));
    if filename_code = mtf_length then
      put_string b loc.filename;
    if defname_code = mtf_length then
      put_string b defname)

let get_backtrace_slot file_mtf defn_mtfs b =
  let id = Int64.to_int (get_64 b) in
  let nlocs = get_8 b in
  let locs = List.init nlocs (fun _ ->
    let low = get_32 b in
    let high = get_16 b in
    let encoded = Int64.(logor (shift_left (of_int high) 32)
                           (logand (of_int32 low) 0xffffffffL)) in
    let line, start_char, end_char, filename_code, defname_code =
      Int64.(
        to_int (logand 0xfffffL encoded),
        to_int (logand 0xffL (shift_right encoded 20)),
        to_int (logand 0x3ffL (shift_right encoded (20 + 8))),
        to_int (logand 0x1fL (shift_right encoded (20 + 8 + 10))),
        to_int (logand 0x1fL (shift_right encoded (20 + 8 + 10 + 5)))) in
    let filename =
      match filename_code with
      | n when n = mtf_length ->
        let s = get_string b in
        mtf_bump file_mtf;
        mtf_swap defn_mtfs (mtf_length - 1);
        file_mtf.(0) <- s;
        s
      | n ->
        mtf_swap defn_mtfs n;
        mtf_swap file_mtf n;
        file_mtf.(0) in
    let defname =
      match defname_code with
      | n when n = mtf_length ->
        let s = get_string b in
        mtf_bump defn_mtfs.(0);
        defn_mtfs.(0).(0) <- s;
        s
      | n ->
        mtf_swap defn_mtfs.(0) n;
        defn_mtfs.(0).(0) in
    (* Printf.fprintf stderr "%d %d %s %s\n" filename_code defname_code filename defname; *)
    { line; start_char; end_char; filename; defname }) in
  (id, locs)


(* CTF packet headers contain several fields that are only known once
   the packet is finished *)

let put_ctf_header b (cache : writer_cache option) =
  put_32 b 0xc1fc1fc1l;
  let off_packet_size = b.pos in
  put_32 b 0l;
  let off_timestamp_begin = b.pos in
  put_64 b 0L;
  let off_timestamp_end = b.pos in
  put_64 b 0L;
  let off_flush_duration = b.pos in
  put_32 b 0l;
  put_16 b memtrace_version;
  put_64 b (Int64.of_int (Unix.getpid ()));
  begin match cache with
  | Some cache -> begin
      let ix = cache.next_verify_ix in
      cache.next_verify_ix <- (cache.next_verify_ix + 5413) land (cache_size - 1);
      put_16 b ix;
      put_16 b cache.cache_next.(ix);
      put_64 b (Int64.of_int cache.cache.(ix));
    end
  | None -> begin
      put_16 b 0xffff;
      put_16 b 0;
      put_64 b 0L;
    end
  end;
  let off_alloc_begin = b.pos in
  put_64 b 0L;
  let off_alloc_end = b.pos in
  put_64 b 0L;
  {hdr_buf=b.buf;
   off_packet_size;
   off_timestamp_begin;
   off_timestamp_end;
   off_flush_duration;
   off_alloc_begin;
   off_alloc_end}

let finish_ctf_header hdr b tstart tend alloc_id_begin alloc_id_end =
  assert (hdr.hdr_buf == b.buf);
  let buf = hdr.hdr_buf in
  let size = b.pos in
  put_raw_32 buf hdr.off_packet_size (Int32.mul (Int32.of_int size) 8l);
  put_raw_64 buf hdr.off_timestamp_begin (to_timestamp_64 tstart);
  put_raw_64 buf hdr.off_timestamp_end (to_timestamp_64 tend);
  put_raw_32 buf hdr.off_flush_duration 0l; (* FIXME flush duration *)
  put_raw_64 buf hdr.off_alloc_begin (Int64.of_int alloc_id_begin);
  put_raw_64 buf hdr.off_alloc_end (Int64.of_int alloc_id_end)



let get_ctf_header b =
  let start = b.pos in
  let magic = get_32 b in
  let packet_size = get_32 b in
  let time_begin = get_64 b in
  let time_end = get_64 b in
  let flush_duration = get_32 b in
  let version = get_16 b in
  let pid = get_64 b in

  let cache_verify_ix = get_16 b in
  let cache_verify_pred = get_16 b in
  let cache_verify_val = get_64 b in
  let alloc_id_begin = get_64 b in
  let alloc_id_end = get_64 b in
  check_fmt "Not a CTF packet" (magic = 0xc1fc1fc1l);
  if version <> memtrace_version then
    bad_formatf "trace format v%03d, but expected v%03d" version memtrace_version;
  check_fmt "Bad packet size" (packet_size >= 0l);
  check_fmt "Monotone packet timestamps" (time_begin <= time_end);
  check_fmt "Monotone alloc IDs" (alloc_id_begin <= alloc_id_end);
  let header_size = b.pos - start in
  {
    content_size = Int32.(to_int (div packet_size 8l) - header_size);
    time_begin;
    time_end;
    alloc_id_begin;
    alloc_id_end;
    cache_verify_ix;
    cache_verify_pred;
    cache_verify_val;
    pid;
  }


let flush s =
  (* First, flush newly-seen locations.
     These must be emitted before any events that might refer to them *)
  let i = ref 0 in
  while !i < s.new_locs_len do
    let b = mkbuffer s.new_locs_buf in
    let hdr = put_ctf_header b None in
    while !i < s.new_locs_len && remaining b > max_location do
      put_event_header b Ev_location s.packet_times.t_start;
      put_backtrace_slot b s.file_mtf s.defn_mtfs s.new_locs.(!i);
      incr i
    done;
    finish_ctf_header hdr b
      s.packet_times.t_start
      s.packet_times.t_start
      s.start_alloc_id
      s.start_alloc_id;
    Deps.write s.dest b.buf 0 b.pos |> ignore
  done;
  (* Next, flush the actual events *)
  finish_ctf_header s.packet_header s.packet
    s.packet_times.t_start
    s.packet_times.t_end
    s.start_alloc_id
    s.next_alloc_id;
  Deps.write s.dest s.packet.buf 0 s.packet.pos |> ignore;
  (* Finally, reset the buffer *)
  s.packet_times.t_start <- s.packet_times.t_end;
  s.new_locs_len <- 0;
  s.packet <- mkbuffer s.packet.buf;
  s.start_alloc_id <- s.next_alloc_id;
  s.packet_header <- put_ctf_header s.packet (Some s.cache)

let max_ev_size = 4096  (* FIXME arbitrary number, overflow *)

let begin_event s ev =
  if remaining s.packet < max_ev_size || s.new_locs_len > 128 then flush s;
  let now = Deps.timestamp () in
  s.packet_times.t_end <- now;
  put_event_header s.packet ev now

let[@inline never] put_bbuf_realloc bbuf pos (x : int) =
  assert (pos = Array.length bbuf);
  let new_size = Array.length bbuf * 2 in
  let new_size = if new_size < 32 then 32 else new_size in
  let new_bbuf = Array.make new_size x in
  Array.blit bbuf 0 new_bbuf 0 pos;
  new_bbuf

let get_coded_backtrace ({ cache_loc ; cache_pred; _ } as cache) bt_length pos b =
  assert (pos <= Array.length cache.last_backtrace);
  let[@inline] put_bbuf bbuf pos (x : int) =
    if pos < Array.length bbuf then begin
      Array.unsafe_set bbuf pos x;
      bbuf
    end else
      put_bbuf_realloc bbuf pos x
    in
  let rec decode pred bbuf pos = function
    | 0 -> (bbuf, pos)
    | i ->
      let codeword = get_16 b in
      let bucket = codeword lsr 2 and tag = codeword land 3 in
      cache_pred.(pred) <- bucket;
      begin match tag with
      | 0 ->
         (* cache hit, 0 prediction *)
         predict bucket
           (put_bbuf bbuf pos cache_loc.(bucket)) (pos + 1)
           (i - 1) 0
      | 1 ->
         (* cache hit, 1 prediction *)
         predict bucket
           (put_bbuf bbuf pos cache_loc.(bucket)) (pos + 1)
           (i - 1) 1
      | 2 ->
         (* cache hit, N prediction *)
         let ncorrect = get_8 b in
         predict bucket
           (put_bbuf bbuf pos cache_loc.(bucket)) (pos + 1)
           (i - 1) ncorrect
      | _ ->
         (* cache miss *)
         let lit = Int64.to_int (get_64 b) in
         cache_loc.(bucket) <- lit;
         decode bucket
           (put_bbuf bbuf pos lit) (pos + 1)
           (i - 1)
      end
  and predict pred bbuf pos i = function
    | 0 -> decode pred bbuf pos i
    | n ->
      let pred' = cache_pred.(pred) in
      predict pred'
        (put_bbuf bbuf pos cache_loc.(pred')) (pos + 1)
        i (n-1) in
  let (bbuf, pos) = decode 0 cache.last_backtrace pos bt_length in
  cache.last_backtrace <- bbuf;
  (bbuf, pos)

let log_alloc s is_major callstack length n_samples =
  (* Find length of common suffix *)
  let raw_stack : int array = Obj.magic callstack in
  let last = s.last_callstack in
  let suff = ref 0 in
  let i = ref (Array.length raw_stack - 1)
  and j = ref (Array.length last - 1) in
  while !i >= 0 && !j >= 0 do
    if Array.unsafe_get raw_stack !i = Array.unsafe_get last !j then begin
      incr suff;
      decr i;
      decr j
    end else begin
      j := -1
    end
  done;

  let is_short =
    1 <= length && length <= 16
    && not is_major
    && n_samples = 1
    && !i < 255 in
  begin_event s (if is_short then Ev_short_alloc length else Ev_alloc);
  let id = s.next_alloc_id in
  s.next_alloc_id <- id + 1;
  s.last_callstack <- raw_stack;
  let cache = s.cache in
  let b = s.packet in
  let common_pfx_len = Array.length raw_stack - 1 - !i in
  let bt_len_off =
    if is_short then begin
      put_vint s.packet common_pfx_len;
      let bt_off = s.packet.pos in
      put_8 b 0;
      bt_off
    end else begin
      put_vint b length;
      put_vint b n_samples;
      put_8 b (if is_major then 1 else 0);
      put_vint b common_pfx_len;
      let bt_off = b.pos in
      put_16 b 0;
      bt_off
    end in
  let bt_elem_off = b.pos in

  let put_hit b bucket ncorrect =
    match ncorrect with
    | 0 -> put_16 b (bucket lsl 2)
    | 1 -> put_16 b ((bucket lsl 2) lor 1)
    | n -> put_16 b ((bucket lsl 2) lor 2); put_8 b n in
  let rec code_no_prediction predictor pos ncodes =
    if pos < 0 then
      ncodes
    else begin
      let mask = cache_size - 1 in
      let slot = raw_stack.(pos) in
      let hash1 = ((slot * 0x4983723) lsr 11) land mask in
      let hash2 = ((slot * 0xfdea731) lsr 21) land mask in
      if cache.cache.(hash1) = slot then begin
        code_cache_hit predictor hash1 pos ncodes
      end else if cache.cache.(hash2) = slot then begin
        code_cache_hit predictor hash2 pos ncodes
      end else begin
        (* cache miss *)
        log_new_loc s (slot, Printexc.get_raw_backtrace_slot callstack pos);
        let bucket =
          if cache.cache_date.(hash1) < cache.cache_date.(hash2) then hash1 else hash2 in
        (* Printf.printf "miss %05d %016x\n%!" bucket slot; (*" %016x\n%!" bucket slot;*) *)
        cache.cache.(bucket) <- slot;
        cache.cache_date.(bucket) <- id;
        cache.cache_next.(predictor) <- bucket;
        put_16 s.packet ((bucket lsl 2) lor 3);
        put_64 s.packet (Int64.of_int slot);
        code_no_prediction bucket (pos-1) (ncodes + 1)
      end
    end
  and code_cache_hit predictor hit pos ncodes =
    (* Printf.printf "hit %d\n" hit; *)
    cache.cache_date.(hit) <- id;
    cache.cache_next.(predictor) <- hit;
    code_with_prediction hit hit 0 (pos-1) (ncodes+1)
  and code_with_prediction orig_hit predictor ncorrect pos ncodes =
    assert (ncorrect < 256);
    if pos < 0 then begin
      put_hit s.packet orig_hit ncorrect;
      ncodes
    end else begin
      let slot = raw_stack.(pos) in
      let pred_bucket = cache.cache_next.(predictor) in
      if cache.cache.(pred_bucket) = slot then begin
        (* correct prediction *)
        (* Printf.printf "pred %d %d\n" pred_bucket ncorrect; *)
        if ncorrect = 255 then begin
          (* overflow: code a new prediction block *)
          put_hit s.packet orig_hit ncorrect;
          code_cache_hit predictor pred_bucket pos ncodes
        end else begin
          code_with_prediction orig_hit pred_bucket (ncorrect + 1) (pos-1) ncodes
        end
      end else begin
        (* incorrect prediction *)
        put_hit s.packet orig_hit ncorrect;
        code_no_prediction predictor pos ncodes
      end
    end in
  let ncodes = code_no_prediction 0 !i 0 in
  if is_short then begin
    assert (ncodes <= 0xff);
    put_raw_8 b.buf bt_len_off ncodes
  end else begin
    (* FIXME: bound this properly *)
    assert (ncodes <= 0xffff);
    put_raw_16 b.buf bt_len_off ncodes
  end;

  (match cache.debug_cache with
   | None -> ()
   | Some c ->
     let b' = { buf = b.buf; pos = bt_elem_off; pos_end = b.pos } in
     let decoded, decoded_len = get_coded_backtrace c ncodes common_pfx_len b' in
     assert (remaining b' = 0);
     if (Array.sub decoded 0 decoded_len) <> (raw_stack |> Array.to_list |> List.rev |> Array.of_list) then begin
    (raw_stack |> Array.map Int64.of_int |> Array.to_list |> List.rev |> Array.of_list) |> Array.iter (Printf.printf " %08Lx"); Printf.printf " !\n%!";

     Array.sub decoded 0 decoded_len |> Array.iter (Printf.printf " %08x"); Printf.printf " !\n%!";
     failwith "bad coded backtrace"
     end);

  Some id

let log_promote s id =
  begin_event s Ev_promote;
  assert (id < s.next_alloc_id);
  let b = s.packet in
  put_vint b (s.next_alloc_id - 1 - id);
  Some id

let log_collect s id =
  begin_event s Ev_collect;
  assert (id < s.next_alloc_id);
  let b = s.packet in
  put_vint b (s.next_alloc_id - 1 - id)


type trace_info = {
  sample_rate : float;
  word_size : int;
  executable_name : string;
  host_name : string;
  ocaml_runtime_params : string;
  pid : Int64.t;
  start_time : Int64.t;
  file_size : Int64.t;
}

let put_trace_info b sample_rate =
  put_float b sample_rate;
  put_8 b Sys.word_size;
  put_string b Sys.executable_name;
  put_string b (Unix.gethostname ());
  put_string b (Sys.runtime_parameters ());
  put_64 b (Int64.of_int (Unix.getpid ()))

let get_trace_info b start_time file_size =
  let sample_rate = get_float b in
  let word_size = get_8 b in
  let executable_name = get_string b in
  let host_name = get_string b in
  let ocaml_runtime_params = get_string b in
  let pid = get_64 b in
  { start_time;
    sample_rate;
    word_size;
    executable_name;
    host_name;
    ocaml_runtime_params;
    pid;
    file_size }

let start_tracing ~sampling_rate ~filename =
  let dest = Deps.open_out filename in
  let now = Deps.timestamp () in
  (* FIXME magic number sizes *)
  let packet_buf = mkbuffer (Bytes.make (1 lsl 15) '\102') in
  begin
    let hdr = put_ctf_header packet_buf None in
    put_event_header packet_buf Ev_trace_info now;
    put_trace_info packet_buf sampling_rate;
    finish_ctf_header hdr packet_buf now now 0 0;
    Deps.write dest packet_buf.buf 0 packet_buf.pos |> ignore;
  end;
  let packet_buf = mkbuffer packet_buf.buf in

  let cache = create_writer_cache () in
  let hdr = put_ctf_header packet_buf (Some cache) in
  let s = {
    dest;
    file_mtf = create_mtf_table ();
    defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ());
    new_locs = [| |];
    new_locs_len = 0;
    (* FIXME magic size *)
    new_locs_buf = Bytes.make 8000 '\102';

    cache;

    last_callstack = [| |];
    next_alloc_id = 0;
    start_alloc_id = 0;
    packet_times = { t_start = now; t_end = now };
    packet_header = hdr;
    packet = packet_buf;

    stopped = false;
    locked = false
  } in
  Deps.memprof_start
    ~callstack_size:max_int
    ~minor_alloc_callback:(fun info ->
      lock_tracer s;
      let r = log_alloc s false info.callstack info.size info.n_samples in
      unlock_tracer s; r)
    ~major_alloc_callback:(fun info ->
      lock_tracer s;
      let r = log_alloc s true info.callstack info.size info.n_samples in
      unlock_tracer s; r)
    ~promote_callback:(fun id ->
      lock_tracer s;
      let r = log_promote s id in
      unlock_tracer s; r)
    ~minor_dealloc_callback:(fun id ->
      lock_tracer s;
      let r = log_collect s id in
      unlock_tracer s; r)
    ~major_dealloc_callback:(fun id ->
      lock_tracer s;
      let r = log_collect s id in
      unlock_tracer s; r)
    ~sampling_rate
    ();
  s

let stop_tracing s =
  if not s.stopped then begin
    s.stopped <- true;
    Deps.memprof_stop ();
    flush s;
    Deps.close_out s.dest
  end

let trace_until_exit ~sampling_rate ~filename =
  let s = start_tracing ~sampling_rate ~filename in
  at_exit (fun () -> stop_tracing s)


let trace_if_requested ?(sampling_rate=0.0001) () =
  match Sys.getenv_opt "MEMTRACE" with
  | None | Some "" -> ()
  | Some filename ->
     (* Prevent spawned OCaml programs from being traced *)
     Unix.putenv "MEMTRACE" "";
     trace_until_exit ~sampling_rate ~filename

type obj_id = int
type timestamp = Int64.t
type location_code = int

type trace = {
  fd : Deps.in_file;
  info : trace_info;
  data_off : int;
  (* FIXME: opt to better hashtable *)
  loc_table : location list IntTbl.t
}

let trace_info { info; _ } = info

let lookup_location { loc_table; _ } code =
  match IntTbl.find loc_table code with
  | v -> v
  | exception Not_found ->
    raise (Invalid_argument "invalid location code")
(*    [{ filename = "<bad>"; line = 0; start_char = 0; end_char = 0; defname = "<bad>" }]*)

type event =
  | Alloc of {
    obj_id : obj_id;
    length : int;
    nsamples : int;
    is_major : bool;
    backtrace_buffer : location_code array;
    backtrace_length : int;
    common_prefix : int;
  }
  | Promote of obj_id
  | Collect of obj_id

let get_alloc ~parse_backtraces evcode cache alloc_id b =
  let is_short, length, nsamples, is_major =
    match evcode with
    | Ev_short_alloc n ->
       true, n, 1, false
    | Ev_alloc -> begin
       let length = get_vint b in
       let nsamples = get_vint b in
       let is_major = get_8 b |> function 0 -> false | 1 -> true | _ -> bad_format "is_major" in
       false, length, nsamples, is_major
      end
    | _ -> assert false in
  let common_prefix = get_vint b in
  let ncoded =
    if is_short then get_8 b else get_16 b in
  let (backtrace_buffer, backtrace_length) =
    if parse_backtraces then
      get_coded_backtrace cache ncoded common_prefix b
    else begin
      for _ = 1 to ncoded do
        let codeword = get_16 b in
        if codeword land 3 = 2 then
          ignore (get_8 b) (* hitN *)
        else if codeword land 3 = 3 then
          ignore (get_64 b) (* miss *)
      done;
      [| |], 0
    end in
  Alloc { obj_id = alloc_id; length; nsamples; is_major;
          backtrace_buffer; backtrace_length; common_prefix }

(* FIXME: overflow, failure to bump end time *)

let get_promote alloc_id b =
  let id_delta = get_vint b in
  check_fmt "promote id sync" (id_delta >= 0);
  let id = alloc_id - 1 - id_delta in
  Promote id

let get_collect alloc_id b =
  let id_delta = get_vint b in
  check_fmt "collect id sync" (id_delta >= 0);
  let id = alloc_id - 1 - id_delta in
  Collect id

let parse_packet_events ~parse_backtraces file_mtf defn_mtfs loc_table cache start_time hdrinfo b f =
  let alloc_id = ref (Int64.to_int hdrinfo.alloc_id_begin) in
  let last_time = ref 0L in
  while remaining b > 0 do
    (* let last_pos = b.pos in *)
    let (ev, time) = get_event_header hdrinfo b in
    check_fmt "monotone timestamps" (!last_time <= time);
    last_time := time;
    let dt = Int64.(sub time start_time) in
    begin match ev with
    | Ev_trace_info ->
       bad_format "Multiple trace-info events present"
    | Ev_location ->
      let (id, loc) = get_backtrace_slot file_mtf defn_mtfs b in
      (*Printf.printf "%3d _ _ location\n" (b.pos - last_pos);*)
      if IntTbl.mem loc_table id then
        check_fmt "consistent location info" (IntTbl.find loc_table id = loc)
      else
        IntTbl.add loc_table id loc
    | (Ev_alloc | Ev_short_alloc _) as evcode ->
      let info = get_alloc ~parse_backtraces evcode cache !alloc_id b in
      incr alloc_id;
      (*Printf.printf "%3d " (b.pos - last_pos);*)
      f dt info
    | Ev_collect ->
      let info = get_collect !alloc_id b in
      (*Printf.printf "%3d " (b.pos - last_pos);*)
      f dt info
    | Ev_promote ->
      let info = get_promote !alloc_id b in
      (*Printf.printf "%3d " (b.pos - last_pos);*)
      f dt info
    end;
    (* Printf.fprintf stderr "%d %d\n" (event_code ev) (b.pos - last_pos); *)
  done;
  check_fmt "alloc id sync" (hdrinfo.alloc_id_end = Int64.of_int (!alloc_id))

let rec read_into fd buf off =
  assert (0 <= off && off <= Bytes.length buf);
  if off = Bytes.length buf then
    { buf; pos = 0; pos_end = off }
  else begin
    let n = Deps.read fd buf off (Bytes.length buf - off) in
    if n = 0 then
      (* EOF *)
      { buf; pos = 0; pos_end = off }
    else
      read_into fd buf (off + n)
  end

let iter_trace {fd; loc_table; data_off; info = { start_time; _ } } ?(parse_backtraces=true) f =
  let cache = create_reader_cache () in
  let file_mtf = create_mtf_table () in
  let defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ()) in
  Unix.lseek fd data_off SEEK_SET |> ignore;
  (* FIXME error handling *)
  let buf = Bytes.make (1 lsl 18) '\000' in
  let refill b =
    let len = remaining b in
    Bytes.blit b.buf b.pos b.buf 0 len;
    read_into fd b.buf len in
  let rec go last_timestamp last_alloc_id b =
    let b = if remaining b < 4096 then refill b else b in
    if remaining b = 0 then () else
    let info = get_ctf_header b in
    if (last_timestamp <= info.time_begin) && (last_alloc_id = info.alloc_id_begin) then begin
      if parse_backtraces && info.cache_verify_ix <> 0xffff then begin
        check_fmt "cache verification" (0 <= info.cache_verify_ix && info.cache_verify_ix < Array.length cache.cache_loc);
        check_fmt "cache verification" (cache.cache_loc.(info.cache_verify_ix) = Int64.to_int info.cache_verify_val);
        check_fmt "cache verification" (cache.cache_pred.(info.cache_verify_ix) = info.cache_verify_pred);
      end;
      let len = info.content_size in
      let b = if remaining b < len then refill b else b in
      if remaining b >= len then begin
        parse_packet_events ~parse_backtraces file_mtf defn_mtfs loc_table cache start_time info
          { b with pos_end = b.pos + len } f;
        go info.time_end info.alloc_id_end { b with pos = b.pos + len }
      end else begin
        Printf.fprintf stderr "skipping truncated final packet\n%!"
      end
    end else begin
      Printf.fprintf stderr "skipping bad packet at id %Ld %Ld-%Ld\n%!" last_alloc_id info.alloc_id_begin info.alloc_id_end;
      go last_timestamp last_alloc_id { b with pos = b.pos + info.content_size }
    end in
  go 0L 0L { buf; pos = 0; pos_end = 0 }

let open_trace ~filename =
  let fd = Deps.open_in filename in
  let file_size = (Unix.LargeFile.fstat fd).st_size in
  (* FIXME magic numbers *)
  let buf = Bytes.make (1 lsl 15) '\000' in
  let b = read_into fd buf 0 in
  let packet_info = get_ctf_header b in
  check_fmt "trace info packet" (remaining b >= packet_info.content_size);
  let ev, evtime = get_event_header packet_info b in
  check_fmt "trace info packet" (ev = Ev_trace_info);
  check_fmt "trace info packet" (evtime = packet_info.time_begin);
  let trace_info = get_trace_info b packet_info.time_begin file_size in
  let loc_table = IntTbl.create 20 in
  { fd; info = trace_info; data_off = b.pos; loc_table }

let close_trace t =
  Deps.close_in t.fd

module IdTbl = IntTbl
