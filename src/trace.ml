open Stdlib_shim
(* This is the implementation of the encoder/decoder for the memtrace
   format. This format is quite involved, and to understand it it's
   best to read the CTF specification and comments in memtrace.tsl
   first. *)

(* Increment this when the format changes in an incompatible way *)
(* Version 2: added context field to trace_info event
   Version 3: added domain field to packet header *)
let memtrace_version = 3

(* If this is true, then all backtraces are immediately decoded and
   verified after encoding. This is slow, but helpful for debugging. *)
let cache_enable_debug = false

open Buf

exception Parse_error of string

let () =
  (Printexc.register_printer [@ocaml.alert "-unsafe_multidomain"]) (function
    | Parse_error s -> Some ("malformed trace: " ^ s)
    | _ -> None)
;;

let[@inline never] bad_format s = raise (Parse_error s)
let[@inline never] bad_formatf f = Printf.ksprintf (fun s -> bad_format s) f
let check_fmt s b = if not b then bad_format s

(* Utility types *)

(* Time since the epoch *)
module Timestamp = struct
  type t = int64

  let of_int64 t = t
  let to_int64 t = t
  let to_float t = Int64.to_float t /. 1_000_000.
  let of_float f = f *. 1_000_000. |> Int64.of_float
  let now () = of_float (Unix.gettimeofday ())
end

(* Time since the start of the trace *)
module Timedelta = struct
  type t = int64

  let to_int64 t = t
  let offset = Int64.add
end

module IntTbl = Hashtbl.MakeSeededPortable (struct
    type t = int

    let hash _seed (id : t) =
      let h = id * 189696287 in
      h lxor (h lsr 23)
    ;;

    (* Required for OCaml >= 5.0.0, but causes errors for older compilers
     because it is an unused value declaration. *)
    let[@warning "-32"] seeded_hash = hash
    let equal (a : t) (b : t) = a = b
  end)

module Domain_id = struct
  type t = int

  module Tbl = IntTbl

  module Expert = struct
    let of_int x = x
  end

  let main_domain = Expert.of_int 0
end

(** CTF packet headers *)

(* Small enough that Unix.write still does single writes.
   (i.e. below 64k) *)
let max_packet_size = 1 lsl 15

type packet_header_info =
  { content_size : int (* bytes, excluding header *)
  ; time_begin : Timestamp.t
  ; time_end : Timestamp.t
  ; alloc_id_begin : Int64.t
  ; alloc_id_end : Int64.t
  ; pid : Int64.t
  ; version : int
  ; domain : int
  ; cache_verifier : Backtrace_codec.Reader.cache_verifier
  }

(* When writing a packet, some fields can be filled in only once the
   packet is complete. *)
type ctf_header_offsets =
  { off_packet_size : Write.position_32
  ; off_timestamp_begin : Write.position_64
  ; off_timestamp_end : Write.position_64
  ; off_flush_duration : Write.position_32
  ; off_alloc_begin : Write.position_64
  ; off_alloc_end : Write.position_64
  }

let put_ctf_header b ~pid ~domain ~cache =
  let open Write in
  put_32 b 0xc1fc1fc1l;
  let off_packet_size = skip_32 b in
  let off_timestamp_begin = skip_64 b in
  let off_timestamp_end = skip_64 b in
  let off_flush_duration = skip_32 b in
  put_16 b memtrace_version;
  put_64 b pid;
  put_16 b domain;
  (match cache with
   | Some c -> Backtrace_codec.Writer.put_cache_verifier c b
   | None -> Backtrace_codec.Writer.put_dummy_verifier b);
  let off_alloc_begin = skip_64 b in
  let off_alloc_end = skip_64 b in
  { off_packet_size
  ; off_timestamp_begin
  ; off_timestamp_end
  ; off_flush_duration
  ; off_alloc_begin
  ; off_alloc_end
  }
;;

let finish_ctf_header hdr b ~timestamp_begin ~timestamp_end ~alloc_id_begin ~alloc_id_end =
  let open Write in
  let size = b.pos in
  update_32 b hdr.off_packet_size (Int32.mul (Int32.of_int size) 8l);
  update_64 b hdr.off_timestamp_begin timestamp_begin;
  update_64 b hdr.off_timestamp_end timestamp_end;
  update_32 b hdr.off_flush_duration 0l;
  update_64 b hdr.off_alloc_begin (Int64.of_int alloc_id_begin);
  update_64 b hdr.off_alloc_end (Int64.of_int alloc_id_end)
;;

let get_ctf_header b =
  let open Read in
  let start = b.pos in
  let magic = get_32 b in
  let packet_size = get_32 b in
  let time_begin = get_64 b in
  let time_end = get_64 b in
  let _flush_duration = get_32 b in
  let version = get_16 b in
  let pid = get_64 b in
  let domain = if version >= 3 then get_16 b else 0 in
  let cache_verifier = Backtrace_codec.Reader.get_cache_verifier b in
  let alloc_id_begin = get_64 b in
  let alloc_id_end = get_64 b in
  check_fmt "Not a CTF packet" (magic = 0xc1fc1fc1l);
  if version > memtrace_version
  then bad_formatf "trace format v%03d, but expected v%03d" version memtrace_version;
  check_fmt "Bad packet size" (packet_size >= 0l);
  check_fmt "Monotone packet timestamps" (time_begin <= time_end);
  check_fmt "Monotone alloc IDs" (alloc_id_begin <= alloc_id_end);
  let header_size = b.pos - start in
  { content_size = Int32.(to_int (div packet_size 8l) - header_size)
  ; time_begin
  ; time_end
  ; alloc_id_begin
  ; alloc_id_end
  ; pid
  ; domain
  ; version
  ; cache_verifier
  }
;;

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
;;

let event_of_code = function
  | 0 -> Ev_trace_info
  | 1 -> Ev_location
  | 2 -> Ev_alloc
  | 3 -> Ev_promote
  | 4 -> Ev_collect
  | n when 101 <= n && n <= 116 -> Ev_short_alloc (n - 100)
  | c -> bad_format ("Unknown event code " ^ string_of_int c)
;;

let event_header_time_len = 25
let event_header_time_mask = 0x1ffffffl

(* NB: packet_max_time is less than (1 lsl event_header_time_len) microsecs *)
let packet_max_time = 30 * 1_000_000

let put_event_header b ev time =
  let open Write in
  let code =
    Int32.(
      logor
        (shift_left (of_int (event_code ev)) event_header_time_len)
        (logand (Int64.to_int32 time) event_header_time_mask))
  in
  put_32 b code
;;

let[@inline] get_event_header info b =
  let open Read in
  let code = get_32 b in
  let start_low = Int32.logand event_header_time_mask (Int64.to_int32 info.time_begin) in
  let time_low = Int32.logand event_header_time_mask code in
  let time_low =
    if time_low < start_low
    then (* Overflow *)
      Int32.(add time_low (of_int (1 lsl event_header_time_len)))
    else time_low
  in
  let time =
    Int64.(
      add
        (logand info.time_begin (lognot (of_int32 event_header_time_mask)))
        (of_int32 time_low))
  in
  check_fmt "time in packet bounds" (info.time_begin <= time && time <= info.time_end);
  let ev =
    event_of_code Int32.(to_int (shift_right_logical code event_header_time_len))
  in
  ev, time
;;

module Location = Location_codec.Location

module Obj_id = struct
  type t = int

  module Tbl = IntTbl

  module Allocator = struct
    type nonrec t =
      { global_ids : t Atomic.t @@ contended
      ; mutable start_id : t (* first object ID this packet *)
      ; mutable next_id : t (* next object ID in this packet *)
      ; mutable last_id : t (* object ID at which we need to reallocate *)
      }

    let has_next t = t.next_id < t.last_id

    let read_next_exn t =
      if t.next_id = t.last_id then failwith "Obj_id.Allocator.next_exn: exhausted";
      t.next_id
    ;;

    let take_next_exn t =
      let id = read_next_exn t in
      t.next_id <- id + 1;
      id
    ;;

    let ids_per_chunk = Atomic.make 10_000

    let new_packet t =
      if not (has_next t)
      then (
        let ids_per_chunk = Atomic.get ids_per_chunk in
        t.next_id <- Atomic.fetch_and_add t.global_ids ids_per_chunk;
        t.last_id <- t.next_id + ids_per_chunk);
      t.start_id <- t.next_id
    ;;

    let of_global_ids global_ids =
      let t = { global_ids; start_id = 0; next_id = 0; last_id = 0 } in
      new_packet t;
      t
    ;;

    let create () = of_global_ids (Atomic.make 0)

    let for_new_domain { global_ids; _ } : (unit -> t) @ portable =
      fun () -> of_global_ids global_ids
    ;;
  end
end

(** Trace info *)

module Info = struct
  type t =
    { sample_rate : float
    ; word_size : int
    ; executable_name : string
    ; host_name : string
    ; ocaml_runtime_params : string
    ; pid : Int64.t
    ; initial_domain : Domain_id.t
    ; start_time : Timestamp.t
    ; context : string option
    }
end

let put_trace_info b (info : Info.t) =
  let open Write in
  put_event_header b Ev_trace_info info.start_time;
  put_float b info.sample_rate;
  put_8 b info.word_size;
  put_string b info.executable_name;
  put_string b info.host_name;
  put_string b info.ocaml_runtime_params;
  put_64 b info.pid;
  let context =
    match info.context with
    | None -> ""
    | Some s -> s
  in
  put_string b context
;;

let get_trace_info b ~packet_info =
  let open Read in
  let start_time = packet_info.time_begin in
  let sample_rate = get_float b in
  let word_size = get_8 b in
  let executable_name = get_string b in
  let host_name = get_string b in
  let ocaml_runtime_params = get_string b in
  let pid = get_64 b in
  let context =
    if packet_info.version >= 2
    then (
      match get_string b with
      | "" -> None
      | s -> Some s)
    else None
  in
  { Info.start_time
  ; sample_rate
  ; word_size
  ; executable_name
  ; host_name
  ; ocaml_runtime_params
  ; pid
  ; initial_domain = packet_info.domain
  ; context
  }
;;

(** Trace writer *)

type writer : value mod portable =
  { dest : Buf.Shared_writer_fd.t
  ; pid : int64
  ; getpid : unit -> int64 @@ portable
  ; domain : Domain_id.t
  ; loc_writer : Location_codec.Writer.t
  ; cache : Backtrace_codec.Writer.t
  ; debug_reader_cache : Backtrace_codec.Reader.t option
  ; (* Locations that missed cache in this packet *)
    mutable new_locs : (int * Location.t list) array
  ; mutable new_locs_len : int
  ; new_locs_buf : Bytes.t
  ; (* Last allocation callstack *)
    mutable last_callstack : int array
  ; (* Number of slots that were dropped from the last callstack *)
    mutable last_dropped_slots : int
  ; obj_ids : Obj_id.Allocator.t
  ; mutable packet_time_start : Timestamp.t
  ; mutable packet_time_end : Timestamp.t
  ; mutable packet_header : ctf_header_offsets
  ; mutable packet : Write.t
  }

let writer_for_domain ~dest ~pid ~getpid ~domain ~obj_ids ~start_time
  : writer @ uncontended
  =
  let packet = Write.of_bytes (Bytes.make max_packet_size '\042') in
  let packet_header = put_ctf_header packet ~pid ~domain ~cache:None in
  let cache = Backtrace_codec.Writer.create () in
  let debug_reader_cache =
    if cache_enable_debug then Some (Backtrace_codec.Reader.create ()) else None
  in
  let s =
    { dest
    ; pid
    ; getpid
    ; domain
    ; loc_writer = Location_codec.Writer.create ()
    ; new_locs = [||]
    ; new_locs_len = 0
    ; new_locs_buf = Bytes.make max_packet_size '\042'
    ; cache
    ; debug_reader_cache
    ; last_callstack = [||]
    ; last_dropped_slots = 0
    ; obj_ids
    ; packet_time_start = start_time
    ; packet_time_end = start_time
    ; packet_header
    ; packet
    }
  in
  s
;;

let make_writer dest ?getpid (info : Info.t) =
  let dest = Buf.Shared_writer_fd.make dest in
  let open Write in
  let getpid =
    match getpid with
    | Some getpid -> getpid
    | None -> fun () -> info.pid
  in
  let pid = getpid () in
  let domain = info.initial_domain in
  let packet = Write.of_bytes (Bytes.make max_packet_size '\042') in
  let obj_ids = Obj_id.Allocator.create () in
  (* Write the trace info packet *)
  (let hdr = put_ctf_header packet ~pid ~domain ~cache:None in
   put_trace_info packet info;
   finish_ctf_header
     hdr
     packet
     ~timestamp_begin:info.start_time
     ~timestamp_end:info.start_time
     ~alloc_id_begin:0
     ~alloc_id_end:0;
   write_fd dest packet);
  writer_for_domain ~dest ~pid ~getpid ~domain ~obj_ids ~start_time:info.start_time
;;

module Location_code = struct
  type t = int

  module Tbl = IntTbl

  module Expert = struct
    let of_int t = t
  end
end

module Allocation_source = struct
  type t =
    | Minor
    | Major
    | External
end

module Event = struct
  type t =
    | Alloc of
        { obj_id : Obj_id.t
        ; length : int
        ; domain : Domain_id.t
        ; nsamples : int
        ; source : Allocation_source.t
        ; backtrace_buffer : Location_code.t array
        ; backtrace_length : int
        ; common_prefix : int
        }
    | Promote of Obj_id.t * Domain_id.t
    | Collect of Obj_id.t * Domain_id.t

  let to_string decode_loc = function
    | Alloc
        { obj_id
        ; length
        ; domain
        ; nsamples
        ; source
        ; backtrace_buffer
        ; backtrace_length
        ; common_prefix
        } ->
      let backtrace =
        List.init backtrace_length (fun i ->
          let s = backtrace_buffer.(i) in
          match decode_loc s with
          | [] -> Printf.sprintf "$%d" (s :> int)
          | ls -> String.concat " " (List.map Location.to_string ls))
        |> String.concat " "
      in
      let alloc_src =
        match source with
        | Minor -> "alloc"
        | Major -> "alloc_major"
        | External -> "alloc_ext"
      in
      Printf.sprintf
        "%010d %s %d len=%d dom=%d % 4d: %s"
        (obj_id :> int)
        alloc_src
        nsamples
        length
        (domain :> int)
        common_prefix
        backtrace
    | Promote (id, _dom) -> Printf.sprintf "%010d promote" (id :> int)
    | Collect (id, _dom) -> Printf.sprintf "%010d collect" (id :> int)
  ;;

  let domain = function
    | Alloc { domain; _ } | Promote (_, domain) | Collect (_, domain) -> domain
  ;;
end

let log_new_loc s loc =
  let alen = Array.length s.new_locs in
  assert (s.new_locs_len <= alen);
  if s.new_locs_len = alen
  then (
    let new_len = if alen = 0 then 32 else alen * 2 in
    let locs = Array.make new_len loc in
    Array.blit s.new_locs 0 locs 0 alen;
    s.new_locs <- locs;
    s.new_locs_len <- alen + 1)
  else (
    s.new_locs.(s.new_locs_len) <- loc;
    s.new_locs_len <- s.new_locs_len + 1)
;;

(** Flushing *)
exception Pid_changed

let flush_at s ~now =
  (* If the PID has changed, then the process forked and we're in the subprocess.
     Don't write anything to the file, and raise an exception to quit tracing *)
  if s.pid <> s.getpid () then raise Pid_changed;
  let open Write in
  (* First, flush newly-seen locations.
     These must be emitted before any events that might refer to them *)
  let i = ref 0 in
  while !i < s.new_locs_len do
    let b = Write.of_bytes s.new_locs_buf in
    let hdr = put_ctf_header b ~pid:s.pid ~domain:s.domain ~cache:None in
    while !i < s.new_locs_len && remaining b > Location_codec.Writer.max_length do
      put_event_header b Ev_location s.packet_time_start;
      Location_codec.Writer.put_location s.loc_writer b s.new_locs.(!i);
      incr i
    done;
    finish_ctf_header
      hdr
      b
      ~timestamp_begin:s.packet_time_start
      ~timestamp_end:s.packet_time_start
      ~alloc_id_begin:s.obj_ids.start_id
      ~alloc_id_end:s.obj_ids.start_id;
    write_fd s.dest b
  done;
  (* Next, flush the actual events *)
  finish_ctf_header
    s.packet_header
    s.packet
    ~timestamp_begin:s.packet_time_start
    ~timestamp_end:s.packet_time_end
    ~alloc_id_begin:s.obj_ids.start_id
    ~alloc_id_end:s.obj_ids.next_id;
  write_fd s.dest s.packet;
  (* Finally, reset the buffer *)
  s.packet_time_start <- now;
  s.packet_time_end <- now;
  s.new_locs_len <- 0;
  s.packet <- Write.of_bytes s.packet.buf;
  Obj_id.Allocator.new_packet s.obj_ids;
  s.packet_header
  <- put_ctf_header s.packet ~pid:s.pid ~domain:s.domain ~cache:(Some s.cache)
;;

let max_ev_size =
  100
  (* upper bound on fixed-size portion of events
         (i.e. not backtraces or locations) *)
  + max Location_codec.Writer.max_length Backtrace_codec.Writer.max_length
;;

let begin_event s ev ~(now : Timestamp.t) =
  let open Write in
  if remaining s.packet < max_ev_size
     || s.new_locs_len > 128
     || Int64.(sub now s.packet_time_start > of_int packet_max_time)
     || not (Obj_id.Allocator.has_next s.obj_ids)
  then flush_at s ~now;
  s.packet_time_end <- now;
  put_event_header s.packet ev now
;;

let flush s = flush_at s ~now:s.packet_time_end

(* Returns length of the longest suffix of curr which is also a suffix of prev *)
let find_common_suffix (prev : int array) prev_start (curr : int array) curr_start =
  assert (prev_start >= 0);
  assert (curr_start >= 0);
  let i = ref (Array.length curr - 1)
  and j = ref (Array.length prev - 1) in
  while !i >= curr_start && !j >= prev_start do
    if Array.unsafe_get curr !i = Array.unsafe_get prev !j
    then (
      decr i;
      decr j)
    else j := -1
  done;
  (* !i is now the highest index of curr that doesn't match prev *)
  Array.length curr - (!i + 1)
;;

type alloc_length_format =
  | Len_short of Write.position_8
  | Len_long of Write.position_16

let put_alloc
  s
  now
  ~length
  ~nsamples
  ~source
  ~callstack
  ~callstack_as_ints
  ~decode_callstack_entry
  ~drop_slots
  =
  let open Write in
  let common_len =
    find_common_suffix s.last_callstack s.last_dropped_slots callstack_as_ints drop_slots
  in
  let new_len = Array.length callstack_as_ints - common_len in
  s.last_callstack <- callstack_as_ints;
  s.last_dropped_slots <- drop_slots;
  let is_short =
    1 <= length
    && length <= 16
    && source = Allocation_source.Minor
    && nsamples = 1
    && new_len < 256
  in
  begin_event s (if is_short then Ev_short_alloc length else Ev_alloc) ~now;
  let id = Obj_id.Allocator.take_next_exn s.obj_ids in
  let cache = s.cache in
  let b = s.packet in
  let src_code =
    match source with
    | Minor -> 0
    | Major -> 1
    | External -> 2
  in
  let bt_len_off =
    if is_short
    then (
      put_vint b common_len;
      Len_short (skip_8 b))
    else (
      put_vint b length;
      put_vint b nsamples;
      put_8 b src_code;
      put_vint b common_len;
      Len_long (skip_16 b))
  in
  let bt_elem_off = b.pos in
  let log_new_location ~index =
    log_new_loc s (callstack_as_ints.(index), decode_callstack_entry callstack index)
  in
  let nencoded =
    Backtrace_codec.Writer.put_backtrace
      cache
      b
      ~alloc_id:id
      ~callstack:callstack_as_ints
      ~callstack_pos:drop_slots
      ~callstack_len:new_len
      ~log_new_location
  in
  (match bt_len_off with
   | Len_short p ->
     assert (nencoded <= 0xff);
     update_8 b p nencoded
   | Len_long p ->
     (* This can't overflow because there isn't room in a packet for more than
        0xffff entries. (See max_packet_size) *)
     assert (nencoded <= 0xffff);
     update_16 b p nencoded);
  (match s.debug_reader_cache with
   | None -> ()
   | Some c ->
     let open Read in
     (* Decode the backtrace and check that it matches *)
     let b' = Read.of_bytes_sub b.buf ~pos:bt_elem_off ~pos_end:b.pos in
     let decoded, decoded_len =
       Backtrace_codec.Reader.get_backtrace c b' ~nencoded ~common_pfx_len:common_len
     in
     assert (remaining b' = 0);
     let rev_callstack =
       callstack_as_ints |> Array.to_list |> List.rev |> Array.of_list
     in
     if Array.sub decoded 0 decoded_len <> rev_callstack
     then (
       rev_callstack |> Array.map Int64.of_int |> Array.iter (Printf.printf " %08Lx");
       Printf.printf " !\n%!";
       Array.sub decoded 0 decoded_len |> Array.iter (Printf.printf " %08x");
       Printf.printf " !\n%!";
       failwith "bad coded backtrace"));
  id
;;

let get_alloc ~parse_backtraces ~domain evcode cache alloc_id b =
  let open Read in
  let is_short, length, nsamples, source =
    match evcode with
    | Ev_short_alloc n -> true, n, 1, Allocation_source.Minor
    | Ev_alloc ->
      let length = get_vint b in
      let nsamples = get_vint b in
      let source : Allocation_source.t =
        match get_8 b with
        | 0 -> Minor
        | 1 -> Major
        | 2 -> External
        | _ -> bad_format "source"
      in
      false, length, nsamples, source
    | _ -> assert false
  in
  let common_pfx_len = get_vint b in
  let nencoded = if is_short then get_8 b else get_16 b in
  let backtrace_buffer, backtrace_length =
    if parse_backtraces
    then Backtrace_codec.Reader.get_backtrace cache b ~nencoded ~common_pfx_len
    else (
      Backtrace_codec.Reader.skip_backtrace cache b ~nencoded ~common_pfx_len;
      [||], 0)
  in
  Event.Alloc
    { obj_id = alloc_id
    ; length
    ; domain
    ; nsamples
    ; source
    ; backtrace_buffer
    ; backtrace_length
    ; common_prefix = common_pfx_len
    }
;;

(* The other events are much simpler *)

let put_promote s now id =
  let open Write in
  begin_event s Ev_promote ~now;
  let b = s.packet in
  put_vint b (s.obj_ids.next_id - 1 - id)
;;

let get_promote ~domain alloc_id b =
  let open Read in
  let id_delta = get_vint b in
  check_fmt "promote id sync" (id_delta >= 0);
  let id = alloc_id - 1 - id_delta in
  Event.Promote (id, domain)
;;

let put_collect s now id =
  let open Write in
  begin_event s Ev_collect ~now;
  let b = s.packet in
  put_vint b (s.obj_ids.next_id - 1 - id)
;;

let get_collect ~domain alloc_id b =
  let open Read in
  let id_delta = get_vint b in
  (* Typically, id_delta >= 0, because you are collecting an object with an earlier object
     ID. However, a tricky case in domain termination (collecting an object previously
     allocated by a now-terminated domain) means that this is not necessarily the case, so
     there's no assertion here *)
  let id = alloc_id - 1 - id_delta in
  Event.Collect (id, domain)
;;

(** Trace reader *)

type reader =
  { fd : Unix.file_descr
  ; info : Info.t
  ; data_off : int
  ; loc_table : Location.t list Location_code.Tbl.t
  }

let make_reader fd =
  let open Read in
  let buf = Bytes.make max_packet_size '\042' in
  let start_pos = Unix.lseek fd 0 SEEK_CUR in
  let b = read_fd fd buf in
  let packet_info = get_ctf_header b in
  let header_size = b.pos in
  let b, _ = split b packet_info.content_size in
  check_fmt "trace info packet size" (remaining b >= packet_info.content_size);
  let ev, evtime = get_event_header packet_info b in
  check_fmt "trace info packet code" (ev = Ev_trace_info);
  check_fmt "trace info packet time" (evtime = packet_info.time_begin);
  let trace_info = get_trace_info b ~packet_info in
  check_fmt "trace info packet done" (remaining b = 0);
  let loc_table = Location_code.Tbl.create 20 in
  let data_off = start_pos + header_size + packet_info.content_size in
  { fd; info = trace_info; data_off; loc_table }
;;

let report_hack fmt = Printf.kfprintf (fun ppf -> Printf.fprintf ppf "\n%!") stderr fmt

let refill_to size fd stream =
  let open Read in
  if remaining stream < size then refill_fd fd stream else stream
;;

let iter s ?(parse_backtraces = true) f =
  let open Read in
  let per_domain = Domain_id.Tbl.create 1 in
  let iter_events_of_packet (packet_header : packet_header_info) b =
    let domain = packet_header.domain in
    let alloc_id = ref (Int64.to_int packet_header.alloc_id_begin) in
    let loc_reader, cache, last_timestamp =
      try Domain_id.Tbl.find per_domain domain with
      | Not_found ->
        let reader = Location_codec.Reader.create () in
        let cache = Backtrace_codec.Reader.create () in
        let last_timestamp = ref s.info.start_time in
        Domain_id.Tbl.add per_domain domain (reader, cache, last_timestamp);
        reader, cache, last_timestamp
    in
    if parse_backtraces
    then
      if not
           (Backtrace_codec.Reader.check_cache_verifier
              cache
              packet_header.cache_verifier)
      then bad_format "cache verification";
    while remaining b > 0 do
      let ev, time = get_event_header packet_header b in
      check_fmt "monotone timestamps" (!last_timestamp <= time);
      last_timestamp := time;
      let dt = Int64.(sub time s.info.start_time) in
      match ev with
      | Ev_trace_info -> bad_format "Multiple trace-info events present"
      | Ev_location ->
        let id, loc = Location_codec.Reader.get_location loc_reader b in
        (*Printf.printf "%3d _ _ location\n" (b.pos - last_pos);*)
        if Location_code.Tbl.mem s.loc_table id
        then
          check_fmt
            "consistent location info"
            (Location_code.Tbl.find s.loc_table id = loc)
        else Location_code.Tbl.add s.loc_table id loc
      | (Ev_alloc | Ev_short_alloc _) as evcode ->
        let info = get_alloc ~parse_backtraces ~domain evcode cache !alloc_id b in
        incr alloc_id;
        (*Printf.printf "%3d " (b.pos - last_pos);*)
        f dt info
      | Ev_collect ->
        let info = get_collect ~domain !alloc_id b in
        (*Printf.printf "%3d " (b.pos - last_pos);*)
        f dt info
      | Ev_promote ->
        let info = get_promote ~domain !alloc_id b in
        (*Printf.printf "%3d " (b.pos - last_pos);*)
        f dt info
    done;
    check_fmt "alloc id sync" (packet_header.alloc_id_end = Int64.of_int !alloc_id)
  in
  Unix.lseek s.fd s.data_off SEEK_SET |> ignore;
  let rec iter_packets stream =
    let header_upper_bound = 200 (* more than big enough for a header *) in
    let stream = refill_to header_upper_bound s.fd stream in
    if remaining stream = 0
    then ()
    else (
      let packet_header = get_ctf_header stream in
      let stream = refill_to packet_header.content_size s.fd stream in
      let packet, rest = split stream packet_header.content_size in
      if packet_header.pid <> s.info.pid
      then
        report_hack
          "skipping bad packet (wrong pid: %Ld, but tracing %Ld)"
          packet_header.pid
          s.info.pid
      else if remaining packet <> packet_header.content_size
      then report_hack "skipping truncated packet"
      else iter_events_of_packet packet_header packet;
      iter_packets rest)
  in
  iter_packets (read_fd s.fd (Bytes.make max_packet_size '\000'))
;;

module Private = struct
  let name_of_memprof_tracer = Atomic.make ""

  let set_name_of_memprof_tracer_module s =
    Atomic.set name_of_memprof_tracer (s ^ ".ext_alloc")
  ;;

  let obj_ids_per_chunk = Obj_id.Allocator.ids_per_chunk
end

module Writer = struct
  type t = writer

  exception Pid_changed = Pid_changed

  let create = make_writer
  let domain t = t.domain

  let for_domain_at_time ~start_time (t @ nonportable uncontended)
    : (domain:int -> t) @ portable
    =
    let { dest; pid; getpid; _ } = t in
    let obj_ids = Obj_id.Allocator.for_new_domain t.obj_ids in
    fun ~domain ->
      let obj_ids @ uncontended = obj_ids () in
      let t @ uncontended =
        writer_for_domain ~dest ~pid ~getpid ~domain ~obj_ids ~start_time
      in
      t
  ;;

  let for_domain t = for_domain_at_time ~start_time:t.packet_time_end t

  (* Unfortunately, efficient access to the backtrace is not possible
     with the current Printexc API, even though internally it's an int
     array. For now, wave the Obj.magic wand. There's a PR to fix this:
     https://github.com/ocaml/ocaml/pull/9663 *)
  let location_code_array_of_raw_backtrace (b : Printexc.raw_backtrace)
    : Location_code.t array
    =
    Obj.magic b
  ;;

  (* Is this a location that we'd prefer to leave out of traces? This mechanism only
     really makes sense for inlinable functions, so that we can drop a single frame out of
     a backtrace slot. For non-inlinable functions like
     [Memprof_tracer.ext_alloc_slowpath], we instead avoid capturing the slot to begin
     with (using [put_alloc_with_suffix_of_raw_backtrace]). *)
  let is_internal_location (loc : Location.t) =
    String.equal loc.defname (Atomic.get Private.name_of_memprof_tracer)
  ;;

  let decode_raw_backtrace_entry callstack i : Location.t list =
    let open Printexc in
    let rec get_locations slot : Location.t list =
      let tail =
        match get_raw_backtrace_next_slot slot with
        | None -> []
        | Some slot -> get_locations slot
      in
      let slot = convert_raw_backtrace_slot slot in
      match Slot.location slot with
      | None -> tail
      | Some { filename; line_number; start_char; end_char; _ } ->
        let defname =
          match Slot.name slot with
          | Some n -> n
          | _ -> "??"
        in
        { filename; line = line_number; start_char; end_char; defname } :: tail
    in
    let locs = get_locations (get_raw_backtrace_slot callstack i) |> List.rev in
    match List.filter (fun loc -> not (is_internal_location loc)) locs with
    | [] ->
      (* It would break things to return an empty list here, and the worst that happens if
         we return the whole list is occasionally slightly confusing output (it looks like
         Memtrace is using memory rather than user code). *)
      locs
    | at_least_one -> at_least_one
  ;;

  let put_alloc_with_suffix_of_raw_backtrace
    t
    now
    ~length
    ~nsamples
    ~source
    ~callstack
    ~drop_slots
    =
    let callstack_as_ints = location_code_array_of_raw_backtrace callstack in
    put_alloc
      t
      now
      ~length
      ~nsamples
      ~source
      ~callstack
      ~callstack_as_ints
      ~decode_callstack_entry:decode_raw_backtrace_entry
      ~drop_slots
  ;;

  let put_alloc_with_raw_backtrace t now ~length ~nsamples ~source ~callstack =
    let callstack_as_ints = location_code_array_of_raw_backtrace callstack in
    put_alloc
      t
      now
      ~length
      ~nsamples
      ~source
      ~callstack
      ~callstack_as_ints
      ~decode_callstack_entry:decode_raw_backtrace_entry
      ~drop_slots:0
  ;;

  let put_alloc t now ~length ~nsamples ~source ~callstack ~decode_callstack_entry =
    let decode_callstack_entry cs i = decode_callstack_entry cs.(i) in
    put_alloc
      t
      now
      ~length
      ~nsamples
      ~source
      ~callstack
      ~callstack_as_ints:callstack
      ~decode_callstack_entry
      ~drop_slots:0
  ;;

  let put_collect = put_collect
  let put_promote = put_promote
  let flush = flush

  let close t =
    flush t;
    Buf.Shared_writer_fd.close t.dest
  ;;

  let put_event w ~decode_callstack_entry now (ev : Event.t) =
    if Event.domain ev <> w.domain
    then raise (Invalid_argument "Trace.put_event: mismatched domain fields");
    if now < w.packet_time_end
    then raise (Invalid_argument "Trace.put_event: out-of-order timestamps");
    match ev with
    | Alloc
        { obj_id
        ; length
        ; domain = _
        ; nsamples
        ; source
        ; backtrace_buffer
        ; backtrace_length
        ; common_prefix = _
        } ->
      let btrev =
        Array.init backtrace_length (fun i -> backtrace_buffer.(backtrace_length - 1 - i))
      in
      let id =
        put_alloc w now ~length ~nsamples ~source ~callstack:btrev ~decode_callstack_entry
      in
      if id <> obj_id then raise (Invalid_argument "Incorrect allocation ID")
    | Promote (id, _domain) -> put_promote w now id
    | Collect (id, _domain) -> put_collect w now id
  ;;

  module Multiplexed_domains = struct
    type nonrec t =
      { mutable last_domain : Domain_id.t
      ; (* Invariant: all writers except possibly that of [last_domain] are flushed *)
        writers : t Domain_id.Tbl.t
      ; start_time : Timestamp.t
      }

    let create dest ?getpid info =
      let w = create dest ?getpid info in
      let writers = Domain_id.Tbl.create 1 in
      let dom = domain w in
      Domain_id.Tbl.add writers dom w;
      { last_domain = dom; writers; start_time = info.start_time }
    ;;

    let writer_for_domain t ~domain =
      let last_w = Domain_id.Tbl.find t.writers t.last_domain in
      if domain = t.last_domain
      then last_w
      else (
        flush last_w;
        t.last_domain <- domain;
        try Domain_id.Tbl.find t.writers domain with
        | Not_found ->
          let w = (for_domain_at_time ~start_time:t.start_time last_w) ~domain in
          Domain_id.Tbl.add t.writers domain w;
          w)
    ;;

    let next_alloc_id t ~domain =
      let w = writer_for_domain t ~domain in
      if not (Obj_id.Allocator.has_next w.obj_ids) then flush_at w ~now:w.packet_time_end;
      Obj_id.Allocator.read_next_exn w.obj_ids
    ;;

    let put_event t ~decode_callstack_entry time ev =
      let w = writer_for_domain t ~domain:(Event.domain ev) in
      put_event w ~decode_callstack_entry time ev
    ;;

    let flush t = flush (Domain_id.Tbl.find t.writers t.last_domain)
  end
end

module Reader = struct
  type t = reader

  let create = make_reader
  let info s = s.info

  let lookup_location_code { loc_table; _ } code =
    match Location_code.Tbl.find loc_table code with
    | v -> v
    | exception Not_found ->
      raise (Invalid_argument (Printf.sprintf "invalid location code %08x" code))
  ;;

  let iter = iter
  let open_ ~filename = make_reader (Unix.openfile filename [ Unix.O_RDONLY ] 0)
  let size_bytes s = (Unix.LargeFile.fstat s.fd).st_size
  let close s = Unix.close s.fd
end
