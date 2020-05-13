(* This is the implementation of the encoder/decoder for the memtrace
   format. This format is quite involved, and to understand it it's
   best to read the CTF specification and comments in memtrace.tsl
   first. *)

(* Increment this when the format changes in an incompatible way *)
let memtrace_version = 1

(* If this is true, then all backtraces are immediately decoded and
   verified after encoding. This is slow, but helpful for debugging. *)
let cache_enable_debug = false

open Buf

exception Parse_error of string
let () =
  Printexc.register_printer (function
    | Parse_error s ->
      Some ("malformed trace: " ^ s)
    | _ -> None)

let[@inline never] bad_format s = raise (Parse_error s)
let[@inline never] bad_formatf f = Printf.ksprintf (fun s -> bad_format s) f
let check_fmt s b = if not b then bad_format s



(** CTF packet headers *)

type timestamp = int64
let timestamp_of_float t =
  t *. 1_000_000. |> Int64.of_float
let float_of_timestamp t =
  (Int64.to_float t) /. 1_000_000.

(* Small enough that Unix.write still does single writes.
   (i.e. below 64k) *)
let max_packet_size = 1 lsl 15

type packet_header_info = {
  content_size: int; (* bytes, excluding header *)
  time_begin : timestamp;
  time_end : timestamp;
  alloc_id_begin : Int64.t;
  alloc_id_end : Int64.t;
  cache_verify_ix : int;
  cache_verify_pred : int;
  cache_verify_val : Int64.t;
  pid : Int64.t
}

(* When writing a packet, some fields can be filled in only once the
   packet is complete. *)
type ctf_header_offsets =
  { off_packet_size : [`Int32] Buf.position;
    off_timestamp_begin : [`Int64] Buf.position;
    off_timestamp_end : [`Int64] Buf.position;
    off_flush_duration : [`Int32] Buf.position;
    off_alloc_begin : [`Int64] Buf.position;
    off_alloc_end : [`Int64] Buf.position }

let put_ctf_header b getpid verify_ix verify_pred verify_val =
  put_32 b 0xc1fc1fc1l;
  let off_packet_size = skip_32 b in
  let off_timestamp_begin = skip_64 b in
  let off_timestamp_end = skip_64 b in
  let off_flush_duration = skip_32 b in
  put_16 b memtrace_version;
  put_64 b (getpid ());
  put_16 b verify_ix;
  put_16 b verify_pred;
  put_64 b verify_val;
  let off_alloc_begin = skip_64 b in
  let off_alloc_end = skip_64 b in
  {off_packet_size;
   off_timestamp_begin;
   off_timestamp_end;
   off_flush_duration;
   off_alloc_begin;
   off_alloc_end}

let finish_ctf_header hdr b tstart tend alloc_id_begin alloc_id_end =
  let size = b.pos in
  update_32 b hdr.off_packet_size (Int32.mul (Int32.of_int size) 8l);
  update_64 b hdr.off_timestamp_begin tstart;
  update_64 b hdr.off_timestamp_end tend;
  update_32 b hdr.off_flush_duration 0l; (* CR sdolan: is flush duration useful? *)
  update_64 b hdr.off_alloc_begin (Int64.of_int alloc_id_begin);
  update_64 b hdr.off_alloc_end (Int64.of_int alloc_id_end)

let get_ctf_header b =
  let start = b.pos in
  let magic = get_32 b in
  let packet_size = get_32 b in
  let time_begin = get_64 b in
  let time_end = get_64 b in
  let _flush_duration = get_32 b in
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



(** Event headers *)

type evcode =
  | Ev_trace_info
  | Ev_location
  | Ev_alloc
  | Ev_promote
  | Ev_collect
  | Ev_short_alloc of int
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
(* NB: packet_max_time is less than (1 lsl event_header_time_len) microsecs *)
let packet_max_time = 30 * 1_000_000


let put_event_header b ev t =
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


(** Move-to-front coding.

    Used to encode filenames and function names in source locations *)

type mtf_table = string array
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


(** Source locations *)

type location = {
  filename : string;
  line : int;
  start_char : int;
  end_char : int;
  defname : string;
}

(* CR sdolan: ensure this can't overflow *)
let max_location = 4 * 1024
let put_backtrace_slot b file_mtf defn_mtfs (id, locs) =
  let max_locs = 255 in
  let locs =
    if List.length locs <= max_locs then locs else
      [ { filename = "<unknown>"; line = 1; start_char = 1; end_char = 1; defname = "??"  } ] in
  assert (List.length locs <= max_locs);
  put_64 b (Int64.of_int id);
  put_8 b (List.length locs);
  locs |> List.iter (fun loc ->
    let clamp n lim = if n < 0 || n > lim then lim else n in
    let line_number = clamp loc.line 0xfffff in
    let start_char = clamp loc.start_char 0xfff in
    let end_char = clamp loc.end_char 0xfff in
    let filename_code = mtf_search file_mtf loc.filename in
    mtf_swap defn_mtfs (if filename_code = mtf_length then
                          mtf_length - 1 else filename_code);
    let defname_code = mtf_search defn_mtfs.(0) loc.defname in
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
      put_string b loc.defname)

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


(** Trace info *)

type trace_info = {
  sample_rate : float;
  word_size : int;
  executable_name : string;
  host_name : string;
  ocaml_runtime_params : string;
  pid : Int64.t;
  start_time : timestamp;
}

let put_trace_info b info =
  put_event_header b Ev_trace_info info.start_time;
  put_float b info.sample_rate;
  put_8 b info.word_size;
  put_string b info.executable_name;
  put_string b info.host_name;
  put_string b info.ocaml_runtime_params;
  put_64 b info.pid

let get_trace_info b start_time =
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
    pid }



(** Trace writer *)

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

(* The writer cache carries slightly more state than the reader cache,
   since the writer must make decisions about which slot to use.
   (The reader just follows the choices made by the writer) *)
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
    debug_cache =
      if cache_enable_debug then Some (create_reader_cache ()) else None;
    next_verify_ix = 0 }


type trace_writer = {
  dest : Unix.file_descr;
  getpid : unit -> int64;
  file_mtf : mtf_table;
  defn_mtfs : mtf_table array;
  cache : writer_cache;

  (* Locations that missed cache in this packet *)
  mutable new_locs : (int * location list) array;
  mutable new_locs_len : int;
  new_locs_buf : Bytes.t;

  (* Last allocation callstack *)
  mutable last_callstack : int array;

  mutable start_alloc_id : int; (* alloc ID at start of packet *)
  mutable next_alloc_id : int;
  mutable packet_time_start : timestamp;
  mutable packet_time_end : timestamp;
  mutable packet_header : ctf_header_offsets;
  mutable packet : Buf.wbuf;
}

let default_getpid () = Int64.of_int (Unix.getpid ())

let make_writer dest ?(getpid=default_getpid) info =
  let packet = Buf.of_bytes (Bytes.make max_packet_size '\042') in
  begin
    (* Write the trace info packet *)
    let hdr = put_ctf_header packet getpid 0xffff 0 0L in
    put_trace_info packet info;
    finish_ctf_header hdr packet info.start_time info.start_time 0 0;
    Buf.write_fd dest packet;
  end;
  let packet = Buf.of_bytes packet.buf in
  let packet_header = put_ctf_header packet getpid 0 0 0L in
  let cache = create_writer_cache () in
  cache.next_verify_ix <- 5413;
  let s =
    { dest;
      getpid;
      file_mtf = create_mtf_table ();
      defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ());
      new_locs = [| |];
      new_locs_len = 0;
      new_locs_buf = Bytes.make max_packet_size '\042';
      cache = cache;
      last_callstack = [| |];
      next_alloc_id = 0;
      start_alloc_id = 0;
      packet_time_start = info.start_time;
      packet_time_end = info.start_time;
      packet_header;
      packet } in
  s


(** Flushing *)

type obj_id = int
type location_code = int
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

let flush_at s now =
  (* First, flush newly-seen locations.
     These must be emitted before any events that might refer to them *)
  let i = ref 0 in
  while !i < s.new_locs_len do
    let b = Buf.of_bytes s.new_locs_buf in
    let hdr = put_ctf_header b s.getpid 0xffff 0 0L in
    while !i < s.new_locs_len && remaining b > max_location do
      put_event_header b Ev_location s.packet_time_start;
      put_backtrace_slot b s.file_mtf s.defn_mtfs s.new_locs.(!i);
      incr i
    done;
    finish_ctf_header hdr b
      s.packet_time_start
      s.packet_time_start
      s.start_alloc_id
      s.start_alloc_id;
    Buf.write_fd s.dest b
  done;
  (* Next, flush the actual events *)
  finish_ctf_header s.packet_header s.packet
    s.packet_time_start
    s.packet_time_end
    s.start_alloc_id
    s.next_alloc_id;
  Buf.write_fd s.dest s.packet;
  (* Finally, reset the buffer *)
  s.packet_time_start <- now;
  s.packet_time_end <- now;
  s.new_locs_len <- 0;
  s.packet <- Buf.of_bytes s.packet.buf;
  s.start_alloc_id <- s.next_alloc_id;
  let ix = s.cache.next_verify_ix in
  s.cache.next_verify_ix <-
    (s.cache.next_verify_ix + 5413) land (cache_size - 1);
  s.packet_header <-
    put_ctf_header s.packet s.getpid
      ix s.cache.cache_next.(ix) (Int64.of_int s.cache.cache.(ix))

(* CR sdolan: make sure that events are actually bounded by this size *)
let max_ev_size = 4096

let begin_event s ev (now : timestamp) =
  if remaining s.packet < max_ev_size
     || s.new_locs_len > 128
     || Int64.(sub now s.packet_time_start > of_int packet_max_time) then
    flush_at s now;
  s.packet_time_end <- now;
  put_event_header s.packet ev now

let flush s = flush_at s s.packet_time_end


(** Event decoding and encoding, including backtraces *)

let[@inline never] realloc_bbuf bbuf pos (x : int) =
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
      realloc_bbuf bbuf pos x
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

type alloc_length_format =
  | Len_short of [`Int8] Buf.position
  | Len_long of [`Int16] Buf.position
let put_alloc s now ~length ~nsamples ~is_major ~callstack ~decode_callstack_entry =
  (* Find length of common suffix *)
  let last = s.last_callstack in
  let suff = ref 0 in
  let i = ref (Array.length callstack - 1)
  and j = ref (Array.length last - 1) in
  while !i >= 0 && !j >= 0 do
    if Array.unsafe_get callstack !i = Array.unsafe_get last !j then begin
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
    && nsamples = 1
    && !i < 255 in
  begin_event s (if is_short then Ev_short_alloc length else Ev_alloc) now;
  let id = s.next_alloc_id in
  s.next_alloc_id <- id + 1;
  s.last_callstack <- callstack;
  let cache = s.cache in
  let b = s.packet in
  let common_pfx_len = Array.length callstack - 1 - !i in
  let bt_len_off =
    if is_short then begin
      put_vint b common_pfx_len;
      Len_short (skip_8 b)
    end else begin
      put_vint b length;
      put_vint b nsamples;
      put_8 b (if is_major then 1 else 0);
      put_vint b common_pfx_len;
      Len_long (skip_16 b)
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
      let slot = callstack.(pos) in
      (* Pick the least recently used of two slots, selected by two
         different hashes. *)
      let hash1 = ((slot * 0x4983723) lsr 11) land mask in
      let hash2 = ((slot * 0xfdea731) lsr 21) land mask in
      if cache.cache.(hash1) = slot then begin
        code_cache_hit predictor hash1 pos ncodes
      end else if cache.cache.(hash2) = slot then begin
        code_cache_hit predictor hash2 pos ncodes
      end else begin
        (* cache miss *)
        log_new_loc s (slot, decode_callstack_entry callstack pos);
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
      let slot = callstack.(pos) in
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
  begin match bt_len_off with
  | Len_short p ->
     assert (ncodes <= 0xff);
     update_8 b p ncodes
  | Len_long p ->
     (* CR sdolan: make sure this can't overflow *)
     assert (ncodes <= 0xffff);
     update_16 b p ncodes
  end;

  (match cache.debug_cache with
   | None -> ()
   | Some c ->
      (* Decode the backtrace and check that it matches *)
     let b' = Buf.of_bytes_sub b.buf ~pos:bt_elem_off ~pos_end:b.pos in
     let decoded, decoded_len = get_coded_backtrace c ncodes common_pfx_len b' in
     assert (remaining b' = 0);
     if (Array.sub decoded 0 decoded_len) <> (callstack |> Array.to_list |> List.rev |> Array.of_list) then begin
    (callstack |> Array.map Int64.of_int |> Array.to_list |> List.rev |> Array.of_list) |> Array.iter (Printf.printf " %08Lx"); Printf.printf " !\n%!";

     Array.sub decoded 0 decoded_len |> Array.iter (Printf.printf " %08x"); Printf.printf " !\n%!";
     failwith "bad coded backtrace"
     end);
  id

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

(* The other events are much simpler *)

let put_promote s now id =
  if id >= s.next_alloc_id then raise (Invalid_argument "Invalid ID in promotion");
  begin_event s Ev_promote now;
  let b = s.packet in
  put_vint b (s.next_alloc_id - 1 - id)

let get_promote alloc_id b =
  let id_delta = get_vint b in
  check_fmt "promote id sync" (id_delta >= 0);
  let id = alloc_id - 1 - id_delta in
  Promote id

let put_collect s now id =
  if id >= s.next_alloc_id then raise (Invalid_argument "Invalid ID in collection");
  begin_event s Ev_collect now;
  let b = s.packet in
  put_vint b (s.next_alloc_id - 1 - id)

let get_collect alloc_id b =
  let id_delta = get_vint b in
  check_fmt "collect id sync" (id_delta >= 0);
  let id = alloc_id - 1 - id_delta in
  Collect id



(** Trace reader *)

module LocTbl = Hashtbl.MakeSeeded (struct
  type t = int
  let hash _seed (id : t) =
    let h = id * 189696287 in
    h lxor (h lsr 23)
  let equal (a : t) (b : t) = a = b
end)

type trace_reader = {
  fd : Unix.file_descr;
  info : trace_info;
  data_off : int;
  loc_table : location list LocTbl.t;
}

let make_reader fd =
  let buf = Bytes.make max_packet_size '\042' in
  let start_pos = Unix.lseek fd 0 SEEK_CUR in
  let b = Buf.read_fd fd buf in
  let packet_info = get_ctf_header b in
  let header_size = b.pos in
  let (b, _) = Buf.split_buf b packet_info.content_size in
  check_fmt "trace info packet size" (remaining b >= packet_info.content_size);
  let ev, evtime = get_event_header packet_info b in
  check_fmt "trace info packet code" (ev = Ev_trace_info);
  check_fmt "trace info packet time" (evtime = packet_info.time_begin);
  let trace_info = get_trace_info b packet_info.time_begin in
  check_fmt "trace info packet done" (remaining b = 0);
  let loc_table = LocTbl.create 20 in
  let data_off = start_pos + header_size + packet_info.content_size in
  { fd;
    info = trace_info;
    data_off;
    loc_table; }

let report_hack fmt =
  Printf.kfprintf (fun ppf -> Printf.fprintf ppf "\n%!") stderr fmt

let refill_to size fd stream =
  if remaining stream < size then Buf.refill_fd fd stream else stream


let cache_verify cache info =
  if info.cache_verify_ix <> 0xffff then begin
    check_fmt "cache verification"
      (0 <= info.cache_verify_ix && info.cache_verify_ix < Array.length cache.cache_loc);
    check_fmt "cache verification"
      (cache.cache_loc.(info.cache_verify_ix) = Int64.to_int info.cache_verify_val);
    check_fmt "cache verification"
      (cache.cache_pred.(info.cache_verify_ix) = info.cache_verify_pred);
  end

type timedelta = int64
let iter_trace s ?(parse_backtraces=true) f =
  let cache = create_reader_cache () in
  let file_mtf = create_mtf_table () in
  let defn_mtfs = Array.init mtf_length (fun _ -> create_mtf_table ()) in
  let last_timestamp = ref s.info.start_time in
  let alloc_id = ref 0 in
  let iter_events_of_packet packet_header b =
    while remaining b > 0 do
      let (ev, time) = get_event_header packet_header b in
      check_fmt "monotone timestamps" (!last_timestamp <= time);
      last_timestamp := time;
      let dt = Int64.(sub time s.info.start_time) in
      begin match ev with
      | Ev_trace_info ->
         bad_format "Multiple trace-info events present"
      | Ev_location ->
        let (id, loc) = get_backtrace_slot file_mtf defn_mtfs b in
        (*Printf.printf "%3d _ _ location\n" (b.pos - last_pos);*)
        begin
          if LocTbl.mem s.loc_table id then
            check_fmt "consistent location info" (LocTbl.find s.loc_table id = loc)
          else
            LocTbl.add s.loc_table id loc
        end
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
      end
    done;
    check_fmt "alloc id sync"
      (packet_header.alloc_id_end = Int64.of_int !alloc_id) in
  Unix.lseek s.fd s.data_off SEEK_SET |> ignore;
  let rec iter_packets stream =
    let header_upper_bound = 200 (* more than big enough for a header *) in
    let stream = refill_to header_upper_bound s.fd stream in
    if remaining stream = 0 then () else
    let packet_header = get_ctf_header stream in
    let stream = refill_to packet_header.content_size s.fd stream in
    let (packet, rest) = Buf.split_buf stream packet_header.content_size in
    if !last_timestamp <= packet_header.time_begin &&
         !alloc_id = Int64.to_int packet_header.alloc_id_begin then begin
      if parse_backtraces then cache_verify cache packet_header;
      if remaining packet = packet_header.content_size then
        iter_events_of_packet packet_header packet
      else
        report_hack "skipping truncated packet"
    end else begin
      report_hack "skipping bad packet at id %d %Ld-%Ld"
        !alloc_id packet_header.alloc_id_begin packet_header.alloc_id_end
    end;
    iter_packets rest in
  iter_packets (Buf.read_fd s.fd (Bytes.make max_packet_size '\000'))


(** Convenience functions and accessors *)

let trace_info s = s.info

let lookup_location { loc_table; _ } code =
  match LocTbl.find loc_table code with
  | v -> v
  | exception Not_found ->
    raise (Invalid_argument "invalid location code")


module IdTbl = LocTbl
let string_of_location { filename; line; start_char; end_char; defname  } =
  Printf.sprintf "%s@%s:%d:%d-%d" defname filename line start_char end_char

let string_of_event decode_loc = function
  | Alloc {obj_id; length; nsamples; is_major;
           backtrace_buffer; backtrace_length; common_prefix} ->
     let backtrace =
       List.init backtrace_length (fun i ->
         let s = backtrace_buffer.(i) in
         match decode_loc s with
         | [] -> Printf.sprintf "$%d" (s :> int)
         | ls -> String.concat " " (List.map string_of_location ls))
       |> String.concat " " in
    Printf.sprintf "%010d %s %d len=%d % 4d: %s"
      (obj_id :> int) (if is_major then "alloc_major" else "alloc")
      nsamples length common_prefix
      backtrace;
  | Promote id ->
     Printf.sprintf "%010d promote" (id :> int)
  | Collect id ->
     Printf.sprintf "%010d collect" (id :> int)

let put_event w ~decode_callstack_entry now ev =
  match ev with
  | Alloc { obj_id; length; nsamples; is_major; backtrace_buffer; backtrace_length; common_prefix = _ } ->
     let btrev = Array.init backtrace_length (fun i ->
                     backtrace_buffer.(backtrace_length - 1 - i)) in
     let id =
       put_alloc w now ~length ~nsamples ~is_major
         ~callstack:btrev
         ~decode_callstack_entry in
     if id <> obj_id then
       raise (Invalid_argument "Incorrect allocation ID")
  | Promote id ->
     put_promote w now id
  | Collect id ->
     put_collect w now id

let open_trace ~filename =
  make_reader (Unix.openfile filename [Unix.O_RDONLY] 0)

let close_trace s =
  Unix.close s.fd

let trace_size_bytes s =
  (Unix.LargeFile.fstat s.fd).st_size

