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
  let reset_in fd = Unix.lseek fd 0 SEEK_SET |> ignore
  let close_in fd = Unix.close fd

  let timestamp () = Unix.gettimeofday ()

  let hostname () = Unix.gethostname ()
  let exec_name () = Sys.executable_name
  let pid () = Unix.getpid ()


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

external put_raw_16 : Bytes.t -> int -> int -> unit = "%caml_bytes_set16u"
external put_raw_32 : Bytes.t -> int -> int32 -> unit = "%caml_bytes_set32u"
external put_raw_64 : Bytes.t -> int -> int64 -> unit = "%caml_bytes_set64u"
external get_raw_16 : Bytes.t -> int -> int = "%caml_bytes_get16"
external get_raw_32 : Bytes.t -> int -> int32 = "%caml_bytes_get32"
external get_raw_64 : Bytes.t -> int -> int64 = "%caml_bytes_get64"
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
let[@inline never] underflow b = raise (Parse_error (`Underflow b.pos))
let[@inline never] bad_format s = raise (Parse_error (`Bad_format s))
let check_fmt s b = if not b then bad_format s


let put_8 b v =
  let pos = b.pos in
  let pos' = b.pos + 1 in
  if pos' > b.pos_end then overflow b else
  (Bytes.unsafe_set b.buf pos (Char.unsafe_chr v);
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

let get_8 b =
  if b.pos + 1 > b.pos_end then underflow b;
  let n = Bytes.unsafe_get b.buf b.pos in
  b.pos <- b.pos + 1;
  Char.code n
let get_16 b =
  if b.pos + 2 > b.pos_end then underflow b;
  let n = get_raw_16 b.buf b.pos in
  b.pos <- b.pos + 2;
  if Sys.big_endian then bswap_16 n else n
let get_32 b =
  if b.pos + 4 > b.pos_end then underflow b;
  let n = get_raw_32 b.buf b.pos in
  b.pos <- b.pos + 4;
  if Sys.big_endian then bswap_32 n else n
let get_64 b =
  if b.pos + 8 > b.pos_end then underflow b;
  let n = get_raw_64 b.buf b.pos in
  b.pos <- b.pos + 8;
  if Sys.big_endian then bswap_64 n else n
(* FIXME: overflow if deserialised on 32-bit. Should I care? *)
let get_vint b =
  match get_8 b with
  | 253 -> get_16 b
  | 254 -> get_32 b |> Int32.to_int
  | 255 -> get_64 b |> Int64.to_int
  | n -> n
let get_string b =
  let start = b.pos in
  while get_8 b <> 0 do () done;
  let len = b.pos - 1 - start in
  Bytes.sub_string b.buf start len
let get_float b =
  Int64.float_of_bits (get_64 b)



type times = { mutable t_start : float; mutable t_end : float }

let cache_size = 1 lsl 15
type cache_bucket = int  (* 0 to cache_size - 1 *)

type memtrace_reader_cache = {
  cache_loc : Int64.t array;
  cache_pred : int array;
}

let create_reader_cache () =
  { cache_loc = Array.make cache_size 0L;
    cache_pred = Array.make cache_size 0 }

type mtf_table = string array

type trace_info = {
  sample_rate : float;
  executable_name : string;
  host_name : string;
  start_time : Int64.t;
  pid : Int32.t
}

type tracer = {
  dest : Deps.out_file;
  trace_info : trace_info;
  file_mtf : mtf_table;
  defn_mtfs : mtf_table array;
  mutable new_locs : (int * Printexc.raw_backtrace_slot) array;
  mutable new_locs_len : int;
  new_locs_buf : Bytes.t;
  mutable last_callstack : int array;

  cache : int array;
  (* when an entry was added to the cache (used for eviction) *)
  cache_date : int array;
  (* last time we saw this entry, which entry followed it? *)
  cache_next : cache_bucket array;

  (* for debugging *)
  debug_cache : memtrace_reader_cache option;

  mutable start_alloc_id : int; (* alloc ID at start of packet *)
  mutable next_alloc_id : int;
  mutable packet_times : times;
  mutable packet : buffer;

  mutable stopped : bool;
}

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
let of_timestamp_64 n =
  Float.of_int (Int64.to_int n) /. 1_000_000.

let to_unix_timestamp = of_timestamp_64

let put_ctf_header b info size tstart tend alloc_id_begin alloc_id_end =
  put_32 b 0xc1fc1fc1l;
  (* CTF sizes are in bits *)
  put_32 b (Int32.mul (Int32.of_int size) 8l);
  put_32 b (Int32.mul (Int32.of_int size) 8l);
  put_64 b (to_timestamp_64 tstart);
  put_64 b (to_timestamp_64 tend);
  put_64 b (Int64.of_int alloc_id_begin);
  put_64 b (Int64.of_int alloc_id_end);
  put_float b info.sample_rate;
  put_string b info.executable_name;
  put_string b info.host_name;
  put_32 b info.pid

type header_info = {
  content_size: int; (* bytes, excluding header *)
  time_begin : Int64.t;
  time_end : Int64.t;
  alloc_id_begin : Int64.t;
  alloc_id_end : Int64.t;
  sample_rate : float;
  executable_name : string;
  host_name : string;
  pid : Int32.t
}
let get_ctf_header b =
  let start = b.pos in
  let magic = get_32 b in
  let packet_size = get_32 b in
  let content_size = get_32 b in
  let time_begin = get_64 b in
  let time_end = get_64 b in
  let alloc_id_begin = get_64 b in
  let alloc_id_end = get_64 b in
  let sample_rate = get_float b in
  let executable_name = get_string b in
  let host_name = get_string b in
  let pid = get_32 b in
  check_fmt "Not a CTF packet" (magic = 0xc1fc1fc1l);
  check_fmt "Bad packet size" (packet_size >= 0l);
  check_fmt "Bad content size" (content_size = packet_size);
  check_fmt "Monotone packet timestamps" (time_begin <= time_end);
  check_fmt "Monotone alloc IDs" (alloc_id_begin <= alloc_id_end);
  let header_size = b.pos - start in
  {
    content_size = Int32.(to_int (div packet_size 8l) - header_size);
    time_begin;
    time_end;
    alloc_id_begin;
    alloc_id_end;
    sample_rate;
    executable_name;
    host_name;
    pid;
  }



type evcode = Ev_location | Ev_alloc | Ev_promote | Ev_collect
let event_code = function
  | Ev_location -> 0
  | Ev_alloc -> 1
  | Ev_promote -> 2
  | Ev_collect -> 3
let event_of_code = function
  | 0 -> Ev_location
  | 1 -> Ev_alloc
  | 2 -> Ev_promote
  | 3 -> Ev_collect
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
let get_event_header info b =
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
  check_fmt "time in packet bounds" (info.time_begin <= time);
  check_fmt "time in packet bounds" (time <= info.time_end);
  let ev = event_of_code (Int32.(to_int (shift_right code
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
  let id = get_64 b in
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

let flush s =
  (* First, flush newly-seen locations.
     These must be emitted before any events that might refer to them *)
  let i = ref 0 in
  while !i < s.new_locs_len do
    let b = mkbuffer s.new_locs_buf in
    put_ctf_header b s.trace_info 0 0. 0. 0 0;
    while !i < s.new_locs_len && remaining b > max_location do
      put_event_header b Ev_location s.packet_times.t_start;
      put_backtrace_slot b s.file_mtf s.defn_mtfs s.new_locs.(!i);
      incr i
    done;
    let blen = b.pos in
    put_ctf_header
      (mkbuffer s.new_locs_buf)
      s.trace_info
      blen
      s.packet_times.t_start
      s.packet_times.t_start
      s.start_alloc_id
      s.start_alloc_id;
    Deps.write s.dest s.new_locs_buf 0 blen |> ignore
  done;
  (* Next, flush the actual events *)
  let evlen = s.packet.pos in
  put_ctf_header
    (mkbuffer s.packet.buf)
    s.trace_info
    evlen
    s.packet_times.t_start
    s.packet_times.t_end
    s.start_alloc_id
    s.next_alloc_id;
  Deps.write s.dest s.packet.buf 0 evlen |> ignore;
  (* Finally, reset the buffer *)
  s.packet_times.t_start <- s.packet_times.t_end;
  s.new_locs_len <- 0;
  s.packet <- mkbuffer s.packet.buf;
  s.start_alloc_id <- s.next_alloc_id;
  put_ctf_header s.packet s.trace_info 0 0. 0. 0 0

let max_ev_size = 4096  (* FIXME arbitrary number, overflow *)

let begin_event s ev =
  if remaining s.packet < max_ev_size || s.new_locs_len > 128 then flush s;
  let now = Deps.timestamp () in
  s.packet_times.t_end <- now;
  put_event_header s.packet ev now


let get_coded_backtrace { cache_loc ; cache_pred } b =
  let rec decode pred acc = function
    | 0 -> List.rev acc
    | i ->
      let codeword = get_16 b in
      let bucket = codeword lsr 1 and tag = codeword land 1 in
      cache_pred.(pred) <- bucket;
      if tag = 0 then begin
        (* cache hit *)
        let ncorrect = get_8 b in
        predict bucket (cache_loc.(bucket) :: acc) (i - 1) ncorrect
      end else begin
        (* cache miss *)
        let lit = get_64 b in
        cache_loc.(bucket) <- lit;
        decode bucket (lit :: acc) (i - 1)
      end
  and predict pred acc i = function
    | 0 -> decode pred acc i
    | n ->
      let pred' = cache_pred.(pred) in
      predict pred' (cache_loc.(pred') :: acc) i (n-1) in
  let n = get_16 b in
  decode 0 [] n

let log_alloc s is_major callstack length n_samples =
  begin_event s Ev_alloc;
  let id = s.next_alloc_id in
  s.next_alloc_id <- id + 1;

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
  s.last_callstack <- raw_stack;

  let b = s.packet in
  let common_pfx_len = Array.length raw_stack - 1 - !i in
  put_vint b length;
  put_vint b n_samples;
  put_8 b (if is_major then 1 else 0);
  put_vint b common_pfx_len;

  let bt_off = b.pos in
  put_16 b 0;
  let rec code_no_prediction predictor pos ncodes =
    if pos < 0 then
      ncodes
    else begin
      let mask = cache_size - 1 in
      let slot = raw_stack.(pos) in
      let hash1 = ((slot * 0x4983723) lsr 11) land mask in
      let hash2 = ((slot * 0xfdea731) lsr 21) land mask in
      if s.cache.(hash1) = slot then begin
        code_cache_hit predictor hash1 pos ncodes
      end else if s.cache.(hash2) = slot then begin
        code_cache_hit predictor hash2 pos ncodes
      end else begin
        (* cache miss *)
        log_new_loc s (slot, Printexc.get_raw_backtrace_slot callstack pos);
        let bucket =
          if s.cache_date.(hash1) < s.cache_date.(hash2) then hash1 else hash2 in
        (* Printf.printf "miss %05d %016x\n%!" bucket slot; (*" %016x\n%!" bucket slot;*) *)
        s.cache.(bucket) <- slot;
        s.cache_date.(bucket) <- id;
        s.cache_next.(predictor) <- bucket;
        put_16 s.packet ((bucket lsl 1) lor 1);
        put_64 s.packet (Int64.of_int slot);
        code_no_prediction bucket (pos-1) (ncodes + 1)
      end
    end
  and code_cache_hit predictor hit pos ncodes =
    (* Printf.printf "hit %d\n" hit; *)
    s.cache_date.(hit) <- id;
    put_16 s.packet (hit lsl 1);
    s.cache_next.(predictor) <- hit;
    code_with_prediction hit 0 (pos-1) (ncodes+1)
  and code_with_prediction predictor ncorrect pos ncodes =
    assert (ncorrect < 256);
    if pos < 0 then begin
      put_8 s.packet ncorrect;
      ncodes
    end else begin
      let slot = raw_stack.(pos) in
      let pred_bucket = s.cache_next.(predictor) in
      if s.cache.(pred_bucket) = slot then begin
        (* correct prediction *)
        (* Printf.printf "pred %d %d\n" pred_bucket ncorrect; *)
        if ncorrect = 255 then begin
          (* overflow: code a new prediction block *)
          put_8 s.packet ncorrect;
          code_cache_hit predictor pred_bucket pos ncodes
        end else begin
          code_with_prediction pred_bucket (ncorrect + 1) (pos-1) ncodes
        end
      end else begin
        (* incorrect prediction *)
        put_8 s.packet ncorrect;
        code_no_prediction predictor pos ncodes
      end
    end in
  let ncodes = code_no_prediction 0 !i 0 in
  (* FIXME: bound this properly *)
  assert (ncodes <= 0xffff);
  put_raw_16 b.buf bt_off ncodes;

  (match s.debug_cache with
   | None -> ()
   | Some c ->
     let b' = { buf = b.buf; pos = bt_off; pos_end = b.pos } in
     let decoded_suff = get_coded_backtrace c b' in
     assert (remaining b' = 0);
     let last = Array.map Int64.of_int last in
     let common_pref =
       Array.sub last (Array.length last - common_pfx_len) common_pfx_len |> Array.to_list |> List.rev in
     let decoded = common_pref @ decoded_suff in
     if decoded <> (raw_stack |> Array.map Int64.of_int |> Array.to_list |> List.rev) then begin
     last |> Array.to_list |> List.rev |> List.iter (Printf.printf " %08Lx"); Printf.printf "\n";
     raw_stack |> Array.to_list |> List.rev |> List.iter (Printf.printf " %08x"); Printf.printf "\n";
     decoded |> List.iter (Printf.printf " %08Lx"); Printf.printf " !\n";
     List.init common_pfx_len (fun _ -> ".") |> List.iter (Printf.printf " %8s");
        decoded_suff |> List.iter (Printf.printf " %08Lx"); Printf.printf "\n%!";
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

let start_tracing ~sampling_rate ~filename =
  let dest = Deps.open_out filename in
  let now = Deps.timestamp () in
  let s = {
    dest;
    trace_info = {
      sample_rate = sampling_rate;
      executable_name = Deps.exec_name ();
      host_name = Deps.hostname ();
      pid = Int32.of_int (Deps.pid ());
      start_time = to_timestamp_64 now
    };
    file_mtf = create_mtf_table ();
    defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ());
    new_locs = [| |];
    new_locs_len = 0;
    (* FIXME magic size *)
    new_locs_buf = Bytes.make 8000 '\102';

    cache = Array.make cache_size 0;
    cache_date = Array.make cache_size 0;
    cache_next = Array.make cache_size 0;
    debug_cache = Some (create_reader_cache ());

    last_callstack = [| |];
    next_alloc_id = 0;
    start_alloc_id = 0;
    packet_times = { t_start = now; t_end = now };
    packet = mkbuffer (Bytes.make 8000 '\102');

    stopped = false;
  } in
  put_ctf_header s.packet s.trace_info 0 0. 0. 0 0;
  Deps.memprof_start
    ~callstack_size:max_int
    ~minor_alloc_callback:(fun info ->
      log_alloc s false info.callstack info.size info.n_samples)
    ~major_alloc_callback:(fun info ->
      log_alloc s true info.callstack info.size info.n_samples)
    ~promote_callback:(fun id -> log_promote s id)
    ~minor_dealloc_callback:(fun id -> log_collect s id)
    ~major_dealloc_callback:(fun id -> log_collect s id)
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


type obj_id = int
type timestamp = Int64.t
type timedelta = Int64.t
type location_code = Int64.t

type trace = {
  fd : Deps.in_file;
  info : trace_info;
  (* FIXME: opt to better hashtable *)
  loc_table : (Int64.t, location list) Hashtbl.t
}

let trace_info { info; _ } = info

let lookup_location { loc_table; _ } code =
  match Hashtbl.find loc_table code with
  | v -> v
  | exception Not_found -> raise (Invalid_argument "invalid location code")

type event =
  | Alloc of {
    obj_id : obj_id;
    length : int;
    nsamples : int;
    is_major : bool;
    common_prefix : int;
    new_suffix : location_code list;
  }
  | Promote of obj_id
  | Collect of obj_id

let get_alloc cache alloc_id b =
  let length = get_vint b in
  let nsamples = get_vint b in
  let is_major = get_8 b |> function 0 -> false | _ -> true in
  let common_prefix = get_vint b in
  let new_suffix = get_coded_backtrace cache b in
  Alloc { obj_id = alloc_id; length; nsamples; is_major; common_prefix; new_suffix }

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

let parse_packet_events file_mtf defn_mtfs loc_table cache start_time hdrinfo b f =
  let alloc_id = ref (Int64.to_int hdrinfo.alloc_id_begin) in
  let last_time = ref 0L in
  while remaining b > 0 do
    (*let last_pos = b.pos in*)
    let (ev, time) = get_event_header hdrinfo b in
    check_fmt "monotone timestamps" (!last_time <= time);
    last_time := time;
    let dt = Int64.(sub time start_time) in
    begin match ev with
    | Ev_location ->
      let (id, loc) = get_backtrace_slot file_mtf defn_mtfs b in
      (*Printf.printf "%3d _ _ location\n" (b.pos - last_pos);*)
      if Hashtbl.mem loc_table id then
        check_fmt "consistent location info" (Hashtbl.find loc_table id = loc)
      else
        Hashtbl.add loc_table id loc
    | Ev_alloc ->
      let info = get_alloc cache !alloc_id b in
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
    end
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

let iter_trace {fd; loc_table; info = { start_time; _ } } f =
  let cache = create_reader_cache () in
  let file_mtf = create_mtf_table () in
  let defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ()) in
  Deps.reset_in fd;
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
    check_fmt "monotone inter-packet times" (last_timestamp <= info.time_begin);
    check_fmt "inter-packet alloc ID" (last_alloc_id = info.alloc_id_begin);
    let len = info.content_size in
    let b = if remaining b < len then refill b else b in
    parse_packet_events file_mtf defn_mtfs loc_table cache start_time info
      { b with pos_end = b.pos + len } f;
    go info.time_end info.alloc_id_end { b with pos = b.pos + len } in
  go 0L 0L { buf; pos = 0; pos_end = 0 }

let open_trace ~filename =
  let fd = Deps.open_in filename in
  (* FIXME magic numbers *)
  let buf = Bytes.make (1 lsl 15) '\000' in
  let b = read_into fd buf 0 in
  let info = get_ctf_header b in
  let info : trace_info = {
    sample_rate = info.sample_rate;
    executable_name = info.executable_name;
    host_name = info.host_name;
    start_time = info.time_begin;
    pid = info.pid
  } in
  let loc_table = Hashtbl.create 20 in
  { fd; info; loc_table }

let close_trace t =
  Deps.close_in t.fd
