(* A generalized suffix tree based on Ukkonen's algorithm
   combined with lossy counting.

   Assumes that individual strings have no duplicate characters,
   and that characters used at the end of strings are only ever
   used at the end of strings.
 *)

open Memtrace

module type Char = sig

  include Hashtbl.HashedType

  val dummy : t

end

module Substring_heavy_hitters (X : Char) : sig

  type t

  val create : error:float -> t

  val insert : t -> common_prefix:int -> X.t array -> count:int -> unit

  val output : t -> frequency:float -> (X.t array * int * int * int) list * int

end = struct

  module Tbl = Hashtbl.Make(X)

  module Node : sig

    type t

    val is_root : t -> bool

    val create_root : unit -> t

    val label : t -> X.t array

    module Queue : sig

      type node = t

      type t

      val create : unit -> t

      (* Iterator that can safely squash the current leaf *)
      val iter : t -> (node -> unit) -> unit

    end

    val add_leaf :
      t -> queue:Queue.t -> array:X.t array -> index:int -> t

    val add_suffix_leaf :
      t -> array:X.t array -> index:int -> t

    val split_edge : parent:t -> child:t -> len:int -> t

    val set_suffix : t -> suffix:t -> unit

    val add_count : t -> int -> unit

    val find_child : t -> X.t -> t option

    val get_child : t -> X.t -> t

    val edge_array : t -> X.t array

    val edge_start : t -> int

    val edge_length : t -> int

    val edge_key : t -> X.t

    val edge_char : t -> int -> X.t

    val has_suffix : t -> bool

    val suffix : t -> t

    val parent : t -> t

    val maybe_squash_leaf : queue:Queue.t -> threshold:int -> t -> unit

    val reset_descendents_count : t -> unit

    val update_parents_descendents_counts : threshold:int -> t -> unit

    val iter_children : (t -> unit) -> t -> unit

    val fold_children : (t -> 'a -> 'a) -> t -> 'a -> 'a

    val is_heavy : threshold:int -> t -> bool

    val output : t -> X.t array * int * int * int

  end = struct

    type t =
      { mutable edge_array : X.t array;
        mutable edge_start : int;
        mutable edge_len : int;
        mutable edge_key : X.t;
        mutable parent : t;
        mutable suffix_link : t;
        mutable kind : kind;
        mutable count : int;
        mutable delta : int;
        mutable max_child_delta : int; }

    and kind =
      | Dummy
      | Front_sentinal of { mutable next : t }
      | Back_sentinal of { mutable previous : t }
      | Root of { children : t Tbl.t; }
      | Thin_branch of
          { mutable children : child_list;
            mutable no_of_children : int;
            mutable incoming : int;
            mutable descendents_count : int;
            mutable heavy_descendents_count : int; }
      | Branch of
          { children : t Tbl.t;
            mutable incoming : int;
            mutable descendents_count : int;
            mutable heavy_descendents_count : int; }
      | Leaf of { mutable next : t; mutable previous: t; }
      | Suffix_leaf of
          { mutable incoming : int;
            mutable descendents_count : int;
            mutable heavy_descendents_count : int; }

    and child_list =
      { key : X.t; mutable child : t; mutable next : child_list }

    type queue =
      { front : t } [@@unboxed]

    let is_root t =
      match t.kind with
      | Root _ -> true
      | _ -> false

    let fat_to_thin = 20
    let thin_to_fat = 30

    let same t1 t2 = t1 == t2

    let dummy_array = [||]

    let dummy =
      let edge_array = dummy_array in
      let edge_start = 0 in
      let edge_len = 0 in
      let edge_key = X.dummy in
      let kind = Dummy in
      let count = 0 in
      let delta = 0 in
      let max_child_delta = 0 in
      let rec t =
        { edge_array; edge_start; edge_len; edge_key;
          parent = t; suffix_link = t; kind;
          count; delta; max_child_delta; }
      in
      t

    let rec dummy_child_list =
      { key = X.dummy; child = dummy; next = dummy_child_list }

    let singleton_child_list key child =
      { key; child; next = dummy_child_list; }

    let label t =
      let rec loop acc t =
        let edge = Array.sub t.edge_array t.edge_start t.edge_len in
        if not (same t t.parent) then loop (edge :: acc) t.parent
        else Array.concat (edge :: acc)
      in
      loop [] t

    let create_root () =
      let edge_array = dummy_array in
      let edge_start = 0 in
      let edge_len = 0 in
      let edge_key = X.dummy in
      let children = Tbl.create 37 in
      let kind = Root { children } in
      let count = 0 in
      let delta = 0 in
      let max_child_delta = 0 in
      let rec node =
        { edge_array; edge_start; edge_len; edge_key;
          parent = node; suffix_link = node; kind;
          count; delta; max_child_delta }
      in
      node

    let next t =
      match t.kind with
      | Dummy | Back_sentinal _ | Suffix_leaf _
      | Root _ | Branch _ | Thin_branch _ ->
          assert false
      | Front_sentinal { next; _ } | Leaf { next; _ } -> next

    let set_next t ~next =
      match t.kind with
      | Dummy | Back_sentinal _ | Suffix_leaf _
      | Root _ | Branch _ | Thin_branch _ ->
          assert false
      | Front_sentinal k -> k.next <- next
      | Leaf k -> k.next <- next

    let set_previous t ~previous =
      match t.kind with
      | Dummy | Front_sentinal _ | Suffix_leaf _
      | Root _ | Branch _ | Thin_branch _ ->
          assert false
      | Back_sentinal k -> k.previous <- previous
      | Leaf k -> k.previous <- previous

    let rec set_child_in_list cl char new_child =
      if X.equal cl.key char then cl.child <- new_child
      else set_child_in_list cl.next char new_child

    let set_child ~parent ~key ~child =
      match parent.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ | Suffix_leaf _ ->
          failwith "set_child: No such child"
      | Root { children } ->
          Tbl.replace children key child
      | Thin_branch { children; _ } ->
          set_child_in_list children key child
      | Branch { children; _ } ->
          Tbl.replace children key child

    let tbl_of_child_list cl =
      let tbl = Tbl.create thin_to_fat in
      let current = ref cl in
      while !current != dummy_child_list do
        let cl = !current in
        Tbl.add tbl cl.key cl.child;
        current := cl.next;
      done;
      tbl

    let add_child ~parent ~key ~child =
      match parent.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf { next; previous } ->
          set_previous next ~previous;
          set_next previous ~next;
          let incoming = 1 in
          let no_of_children = 1 in
          let children = singleton_child_list key child in
          let descendents_count = 0 in
          let heavy_descendents_count = 0 in
          let new_kind =
            Thin_branch
              { children; no_of_children; incoming;
                descendents_count; heavy_descendents_count }
          in
          parent.kind <- new_kind
      | Suffix_leaf { incoming; descendents_count; heavy_descendents_count } ->
          let incoming = incoming + 1 in
          let no_of_children = 1 in
          let children = singleton_child_list key child in
          let new_kind =
            Thin_branch
              { children; no_of_children; incoming;
                descendents_count; heavy_descendents_count }
          in
          parent.kind <- new_kind
      | Root { children } -> Tbl.add children key child
      | Thin_branch ({ children; no_of_children; incoming; _ } as k) ->
          let no_of_children = no_of_children + 1 in
          if no_of_children >= thin_to_fat then begin
            let children = tbl_of_child_list children in
            Tbl.add children key child;
            let descendents_count = k.descendents_count in
            let heavy_descendents_count = k.heavy_descendents_count in
            let new_kind =
              Branch
                { children; incoming;
                  descendents_count; heavy_descendents_count }
            in
            parent.kind <- new_kind
          end else begin
            let next = children in
            let children = { key; child; next } in
            k.children <- children;
            k.incoming <- incoming + 1;
            k.no_of_children <- no_of_children
          end
      | Branch ({ children; incoming; _ } as k) ->
          Tbl.add children key child;
          k.incoming <- incoming + 1

    let convert_to_leaf ~queue t =
      let previous = queue.front in
      let next = next queue.front in
      let new_kind = Leaf { next; previous } in
      t.kind <- new_kind;
      set_previous next ~previous:t;
      set_next previous ~next:t

    let child_list_of_tbl tbl =
      Tbl.fold (fun key child next -> { key; child; next })
        tbl dummy_child_list

    let rec remove_from_child_list previous cl char =
      if X.equal cl.key char then begin
        previous.next <- cl.next;
      end else begin
        remove_from_child_list cl cl.next char
      end

    let remove_child ~parent ~child =
      let key = child.edge_key in
      match parent.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ | Leaf _ | Suffix_leaf _ ->
          assert false
      | Root { children } ->
          Tbl.remove children key; false
      | Thin_branch ({ children; incoming; _ } as k) ->
          if X.equal children.key key then begin
            k.children <- children.next
          end else begin
            remove_from_child_list children children.next key
          end;
          let incoming = incoming - 1 in
          if incoming = 0 then true
          else begin
            let no_of_children = k.no_of_children - 1 in
            if no_of_children = 0 then begin
              let descendents_count = k.descendents_count in
              let heavy_descendents_count = k.heavy_descendents_count in
              let new_kind =
                Suffix_leaf
                  { incoming;
                    descendents_count; heavy_descendents_count; }
              in
              parent.kind <- new_kind
            end else begin
              k.incoming <- incoming;
              k.no_of_children <- no_of_children
            end;
            false
          end
      | Branch ({ children; incoming; _ } as k) ->
          Tbl.remove children key;
          let no_of_children = Tbl.length children in
          if no_of_children <= fat_to_thin then begin
            let children = child_list_of_tbl children in
            let descendents_count = k.descendents_count in
            let heavy_descendents_count = k.heavy_descendents_count in
            let new_kind =
              Thin_branch
                { children; no_of_children; incoming;
                  descendents_count; heavy_descendents_count; }
            in
            parent.kind <- new_kind
          end else begin
            let incoming = incoming - 1 in
            k.incoming <- incoming
          end;
          false

    let set_suffix t ~suffix =
      t.suffix_link <- suffix;
      match suffix.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Root _ -> ()
      | Thin_branch k ->
          k.incoming <- k.incoming + 1
      | Branch k ->
          k.incoming <- k.incoming + 1
      | Suffix_leaf k ->
          k.incoming <- k.incoming + 1
      | Leaf { next; previous } ->
          set_previous next ~previous;
          set_next previous ~next;
          let incoming = 1 in
          let descendents_count = 0 in
          let heavy_descendents_count = 0 in
          let new_kind =
            Suffix_leaf
              { incoming; descendents_count; heavy_descendents_count; }
          in
          suffix.kind <- new_kind

    let remove_incoming t =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ | Leaf _ ->
          assert false
      | Root _ -> false
      | Suffix_leaf ({ incoming; _ } as k) ->
          let incoming = incoming - 1 in
          if incoming = 0 then true
          else begin
            k.incoming <- incoming;
            false
          end
      | Thin_branch ({ incoming; _ } as k) ->
          let incoming = incoming - 1 in
          k.incoming <- incoming;
          false
      | Branch ({ incoming; _ } as k) ->
          let incoming = incoming - 1 in
          k.incoming <- incoming;
          false

    let add_leaf t ~queue ~array ~index =
      let edge_array = array in
      let edge_start = index in
      let edge_len = (Array.length array) - index in
      let edge_key = edge_array.(edge_start) in
      let parent = t in
      let suffix_link = dummy in
      let next = next queue.front in
      let previous = queue.front in
      let kind = Leaf { next; previous } in
      let count = 0 in
      let max_child_delta = t.max_child_delta in
      let delta = max_child_delta in
      let node =
        { edge_array; edge_start; edge_len; edge_key;
          parent; suffix_link; kind;
          count; delta; max_child_delta; }
      in
      set_previous next ~previous:node;
      set_next previous ~next:node;
      add_child ~parent:t ~key:edge_key ~child:node;
      node

    let add_suffix_leaf t ~array ~index =
      let edge_array = array in
      let edge_start = index in
      let edge_len = (Array.length array) - index in
      let edge_key = edge_array.(edge_start) in
      let parent = t in
      let suffix_link = dummy in
      let incoming = 0 in
      let descendents_count = 0 in
      let heavy_descendents_count = 0 in
      let kind =
        Suffix_leaf
          { incoming; descendents_count; heavy_descendents_count; }
      in
      let count = 0 in
      let max_child_delta = t.max_child_delta in
      let delta = max_child_delta in
      let node =
        { edge_array; edge_start; edge_len; edge_key;
          parent; suffix_link; kind;
          count; delta; max_child_delta; }
      in
      add_child ~parent:t ~key:edge_key ~child:node;
      node

    let split_edge ~parent ~child ~len =
      if (len = 0) then parent
      else begin
        let edge_array = child.edge_array in
        let edge_start = child.edge_start in
        let edge_key = child.edge_key in
        let child_key = edge_array.(edge_start + len) in
        let new_node =
          let edge_len = len in
          let suffix_link = dummy in
          let incoming = 1 in
          let no_of_children = 1 in
          let children = singleton_child_list child_key child in
          let descendents_count = 0 in
          let heavy_descendents_count = 0 in
          let kind =
            Thin_branch
              { children; no_of_children; incoming;
                descendents_count; heavy_descendents_count }
          in
          let count = 0 in
          let max_child_delta = parent.max_child_delta in
          let delta = max_child_delta in
          { edge_array; edge_start; edge_len; edge_key;
            parent; suffix_link; kind;
            count; delta; max_child_delta}
        in
        child.edge_start <- edge_start + len;
        child.edge_len <- child.edge_len - len;
        child.edge_key <- child_key;
        child.parent <- new_node;
        set_child ~parent ~key:edge_key ~child:new_node;
        new_node
      end

    let add_count t count =
      t.count <- t.count + count

    let add_child_delta t delta =
      if delta > t.max_child_delta then begin
        t.max_child_delta <- delta
      end

    let rec find_child_in_list cl char =
      if cl == dummy_child_list then None
      else if X.equal cl.key char then Some cl.child
      else find_child_in_list cl.next char

    let find_child t char =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ | Suffix_leaf _ -> None
      | Thin_branch { children; _ } ->
          find_child_in_list children char
      | Root { children; _ } | Branch { children; _ } ->
          Tbl.find_opt children char

    let rec get_child_in_list cl char =
      if X.equal cl.key char then cl.child
      else get_child_in_list cl.next char

    let get_child t char =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ | Suffix_leaf _ -> failwith "get_child: No children"
      | Thin_branch { children; _ } -> get_child_in_list children char
      | Root { children; _ } | Branch { children; _ } ->
          match Tbl.find children char with
          | child -> child
          | exception Not_found -> failwith "get_child: No such child"

    let edge_array t = t.edge_array

    let edge_start t = t.edge_start

    let edge_length t = t.edge_len

    let edge_key t = t.edge_key

    let edge_char t i =
      if i = 0 then t.edge_key
      else t.edge_array.(t.edge_start + i)

    let has_suffix t = not (same t.suffix_link dummy)

    let suffix t = t.suffix_link

    let parent t = t.parent

    let reset_descendents_count t =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ | Root _ -> ()
      | Suffix_leaf k ->
        k.descendents_count <- 0;
        k.heavy_descendents_count <- 0
      | Thin_branch k ->
        k.descendents_count <- 0;
        k.heavy_descendents_count <- 0
      | Branch k ->
        k.descendents_count <- 0;
        k.heavy_descendents_count <- 0

    let add_to_descendents_count t diff =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ -> assert false
      | Suffix_leaf k -> k.descendents_count <- k.descendents_count + diff
      | Thin_branch k -> k.descendents_count <- k.descendents_count + diff
      | Branch k -> k.descendents_count <- k.descendents_count + diff
      | Root _ -> ()

    let add_to_heavy_descendents_count t diff =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ -> assert false
      | Suffix_leaf k ->
          k.heavy_descendents_count <- k.heavy_descendents_count + diff
      | Thin_branch k ->
          k.heavy_descendents_count <- k.heavy_descendents_count + diff
      | Branch k -> k.heavy_descendents_count <- k.heavy_descendents_count + diff
      | Root _ -> ()

    let descendents_count t =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ -> 0
      | Suffix_leaf { descendents_count; _ }
      | Thin_branch { descendents_count; _ }
      | Branch { descendents_count; _ } -> descendents_count
      | Root _ -> assert false

    let heavy_descendents_count t =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ -> 0
      | Suffix_leaf { heavy_descendents_count; _ }
      | Thin_branch { heavy_descendents_count; _ }
      | Branch { heavy_descendents_count; _ } -> heavy_descendents_count
      | Root _ -> assert false

    let update_parents_descendents_counts ~threshold t =
      if is_root t then ()
      else begin
        let heavy_descendents_count = heavy_descendents_count t in
        let descendents_count = descendents_count t in
        let total = t.count + descendents_count in
        let light_total = total - heavy_descendents_count in
        let heavy_total =
          if light_total + t.delta > threshold then total
          else heavy_descendents_count
        in
        let parent = t.parent in
        let suffix = t.suffix_link in
        let grand_parent = t.parent.suffix_link in
        if not (same parent dummy) then begin
          add_to_descendents_count parent total;
          add_to_heavy_descendents_count parent heavy_total
        end;
        if not (same suffix dummy) then begin
          add_to_descendents_count suffix total;
          add_to_heavy_descendents_count suffix heavy_total
        end;
        if not (same grand_parent dummy) then begin
          add_to_descendents_count grand_parent (-total);
          add_to_heavy_descendents_count grand_parent (-heavy_total)
        end
      end

    let is_heavy ~threshold t =
      let heavy_descendents_count = heavy_descendents_count t in
      let descendents_count = descendents_count t in
      let total = t.count + descendents_count in
      let light_total = total - heavy_descendents_count in
      light_total + t.delta > threshold

    let output t =
      let heavy_descendents_count = heavy_descendents_count t in
      let descendents_count = descendents_count t in
      let total = t.count + descendents_count in
      let light_total = total - heavy_descendents_count in
      (label t, light_total, total, total + t.delta)

    let iter_over_child_list f cl =
      let current = ref cl in
      while !current != dummy_child_list do
        let cl = !current in
        f cl.child;
        current := cl.next;
      done

    let iter_children f t =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ | Suffix_leaf _ -> ()
      | Thin_branch { children; _ } ->
          iter_over_child_list f children
      | Root { children; _ } | Branch { children; _ } ->
          Tbl.iter (fun _ n -> f n) children

    let fold_over_child_list f cl acc =
      let acc = ref acc in
      let current = ref cl in
      while !current != dummy_child_list do
        let cl = !current in
        acc := f cl.child !acc;
        current := cl.next;
      done;
      !acc

    let fold_children f t acc =
      match t.kind with
      | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
      | Leaf _ | Suffix_leaf _ -> acc
      | Thin_branch { children; _ } ->
          fold_over_child_list f children acc
      | Root { children; _ } | Branch { children; _ } ->
          Tbl.fold (fun _ n -> f n) children acc

    let rec squash_detached ~queue ~threshold t =
      let parent = t.parent in
      let suffix = t.suffix_link in
      let count = t.count in
      let delta = t.delta in
      let grand_parent = t.parent.suffix_link in
      add_count parent count;
      add_child_delta parent (count + delta);
      add_count grand_parent (-count);
      if (remove_child ~parent ~child:t) then begin
        let upper_bound = parent.count + parent.delta in
        if upper_bound < threshold then squash_detached ~queue ~threshold parent
        else convert_to_leaf ~queue parent
      end;
      add_count suffix count;
      if (remove_incoming suffix) then begin
        let upper_bound = suffix.count + suffix.delta in
        if upper_bound < threshold then squash_detached ~queue ~threshold suffix
        else convert_to_leaf ~queue suffix
      end

    let maybe_squash_leaf ~queue ~threshold t =
      let upper_bound = t.count + t.delta in
      if upper_bound < threshold then begin
        match t.kind with
        | Dummy | Front_sentinal _ | Back_sentinal _ -> assert false
        | Root _ | Thin_branch _
        | Branch _ | Suffix_leaf _ -> failwith "squash: Not a leaf"
        | Leaf ({ previous; next } as k) ->
            set_previous next ~previous;
            set_next previous ~next;
            k.next <- dummy;
            k.previous <- dummy;
            squash_detached ~queue ~threshold t
      end

    module Queue = struct

      type node = t

      type t = queue

      let create () =
        let edge_array = dummy_array in
        let edge_start = 0 in
        let edge_len = 0 in
        let edge_key = X.dummy in
        let count = 0 in
        let delta = 0 in
        let max_child_delta = 0 in
        let parent = dummy in
        let suffix_link = dummy in
        let front =
          let next = dummy in
          let kind = Front_sentinal { next; } in
          { edge_array; edge_start; edge_len; edge_key;
            parent; suffix_link; kind;
            count; delta; max_child_delta; }
        in
        let back =
          let previous = front in
          let kind = Back_sentinal { previous; } in
          { edge_array; edge_start; edge_len; edge_key;
            parent; suffix_link; kind;
            count; delta; max_child_delta; }
        in
        set_next front ~next:back;
        { front }

      let is_back_sentinal t =
        match t.kind with
        | Back_sentinal _ -> true
        | _ -> false

      let iter t f =
        let current = ref (next t.front) in
        while not (is_back_sentinal !current) do
          let next_current = next !current in
          f !current;
          current := next_current
        done

    end

  end

  module Cursor : sig

    type t

    val create : at:Node.t -> t

    val goto : t -> Node.t -> unit

    val retract : t -> distance:int -> unit

    val scan : t -> array:X.t array -> index:int -> bool

    val split_at : t -> Node.t

    val goto_suffix : t -> Node.t -> unit

  end = struct

    type t =
      { mutable parent : Node.t;
        mutable len : int;
        mutable child : Node.t; } (* = parent if len is 0 *)

    let create ~at =
      { parent = at;
        len = 0;
        child = at; }

    let goto t node =
      t.parent <- node;
      t.len <- 0;
      t.child <- node

    let rec retract t ~distance =
      let len = t.len in
      if len > distance then begin
        t.len <- len - distance
      end else if len = distance then begin
        t.len <- 0;
        t.child <- t.parent;
      end else begin
        let distance = distance - len in
        let parent = t.parent in
        t.child <- parent;
        t.parent <- Node.parent parent;
        t.len <- Node.edge_length parent;
        retract t ~distance
      end

    (* Move cursor 1 character towards child. Child assumed
       not equal to parent. *)
    let extend t =
      let len = t.len + 1 in
      if (Node.edge_length t.child) <= len then begin
        t.parent <- t.child;
        t.len <- 0
      end else begin
        t.len <- len
      end

    (* Try to move cursor n character towards child. Child assumed
       not equal to parent. Returns number of characters actually moved.
       Guaranteed to move at least 1 character. *)
    let extend_n t n =
      let len = t.len in
      let target = len + n in
      let edge_len = Node.edge_length t.child in
      if edge_len <= target then begin
        t.parent <- t.child;
        t.len <- 0;
        edge_len - len
      end else begin
        t.len <- target;
        n
      end

    let scan t ~array ~index =
      let char = array.(index) in
      let len = t.len in
      if len = 0 then begin
        match Node.find_child t.parent char with
        | None -> false
        | Some child ->
            t.child <- child;
            extend t;
            true
      end else begin
        let next_char = Node.edge_char t.child len in
        if X.equal char next_char then begin
          extend t;
          true
        end else begin
          false
        end
      end

    let split_at t =
      let len = t.len in
      if len = 0 then t.parent
      else begin
        let node = Node.split_edge ~parent:t.parent ~child:t.child ~len in
        goto t node;
        node
      end

    let rec rescan t ~array ~start ~len =
      if len = 0 then ()
      else begin
        if t.len = 0 then begin
          let char = array.(start) in
          let child = Node.get_child t.parent char in
          t.child <- child
        end;
        let diff = extend_n t len in
        let start = start + diff in
        let len = len - diff in
        rescan t ~array ~start ~len
      end

    let rescan1 t ~key =
      if t.len = 0 then begin
        let child = Node.get_child t.parent key in
        t.child <- child
      end;
      extend t

    let rec goto_suffix t node =
      if Node.is_root node then begin
        goto t node
      end else if Node.has_suffix node then begin
        goto t (Node.suffix node)
      end else begin
        let parent = Node.parent node in
        let len = Node.edge_length node in
        if len = 1 then begin
          if Node.is_root parent then begin
            goto t parent
          end else begin
            let key = Node.edge_key node in
            goto_suffix t parent;
            rescan1 t ~key
          end
        end else begin
          let array = Node.edge_array node in
          let start = Node.edge_start node in
          if Node.is_root parent then begin
            goto t parent;
            let start = start + 1 in
            let len = len - 1 in
            rescan t ~array ~start ~len
          end else begin
            goto_suffix t parent;
            rescan t ~array ~start ~len
          end
        end
      end

  end

  type state =
    | Uncompressed
    | Compressed of X.t array

  type t =
    { root : Node.t;
      leaves : Node.Queue.t;
      mutable max_length : int;
      mutable count : int;
      bucket_size : int;
      mutable current_bucket : int;
      mutable remaining_in_current_bucket : int;
      active : Cursor.t;
      mutable previous_length : int;
      mutable state : state;
    }

  let create ~error =
    let root = Node.create_root () in
    let leaves = Node.Queue.create () in
    let max_length = 0 in
    let count = 0 in
    let bucket_size = Float.to_int (Float.ceil (1.0 /. error)) in
    let current_bucket = 0 in
    let remaining_in_current_bucket = bucket_size in
    let active = Cursor.create ~at:root in
    let previous_length = 0 in
    let state = Uncompressed in
    { root; leaves; max_length; count;
      bucket_size; current_bucket; remaining_in_current_bucket;
      active; previous_length; state }

  let update_descendent_counts ~threshold t =
    let nodes : Node.t list array = Array.make (t.max_length + 1) [] in
    let rec loop depth node =
      Node.reset_descendents_count node;
      let depth = depth + Node.edge_length node in
      nodes.(depth) <- node :: nodes.(depth);
      Node.iter_children (loop depth) node
    in
    loop 0 t.root;
    for i = t.max_length downto 0 do
      List.iter (Node.update_parents_descendents_counts ~threshold) nodes.(i)
    done

  let compress t =
    let queue = t.leaves in
    let threshold = t.current_bucket in
    Node.Queue.iter queue (Node.maybe_squash_leaf ~queue ~threshold)

  let insert t ~common_prefix array ~count =
    let len = Array.length array in
    let total_len = common_prefix + len in
    if total_len > t.max_length then t.max_length <- total_len;
    t.count <- t.count + count;
    let queue = t.leaves in
    let active = t.active in
    let array, len, base =
      match t.state with
      | Uncompressed ->
        Cursor.retract active ~distance:(t.previous_length - common_prefix);
        array, len, common_prefix
      | Compressed previous_label ->
        let common = Array.sub previous_label 0 common_prefix in
        let array = Array.append common array in
        array, total_len, 0
    in
    let rec loop array len base queue active index j is_suffix =
      if index >= len then begin
        Cursor.split_at active
      end else begin
        loop_inner array len base queue active index j is_suffix
      end
    and loop_inner array len base queue active index j is_suffix =
      if j > base + index then begin
        loop array len base queue active (index + 1) j is_suffix
      end else if Cursor.scan active ~array ~index then begin
        loop array len base queue active (index + 1) j is_suffix
      end else begin
        let parent = Cursor.split_at active in
        Cursor.goto_suffix active parent;
        let leaf =
          if is_suffix then
            Node.add_suffix_leaf parent ~array ~index
          else
            Node.add_leaf parent ~queue ~array ~index
        in
        if not (Node.has_suffix parent) then begin
          let suffix = Cursor.split_at active in
          Node.set_suffix parent ~suffix
        end;
        let leaf_suffix =
          loop_inner array len base queue active index (j + 1) true
        in
        Node.set_suffix leaf ~suffix:leaf_suffix;
        leaf
      end
    in
    let destination = loop array len base queue active 0 0 false in
    Node.add_count destination count;
    let remaining = t.remaining_in_current_bucket - 1 in
    if remaining <= 0 then begin
      t.current_bucket <- t.current_bucket + 1;
      t.remaining_in_current_bucket <- t.bucket_size;
      let destination_label = Node.label destination in
      Cursor.goto active t.root;
      compress t;
      t.previous_length <- 0;
      t.state <- Compressed destination_label
    end else begin
      t.remaining_in_current_bucket <- remaining;
      Cursor.goto active destination;
      t.previous_length <- total_len;
      t.state <- Uncompressed
    end

  let output t ~frequency =
    let threshold =
        Float.to_int (Float.floor (frequency *. (Float.of_int t.count)))
    in
    let cmp (_, (light_count1 : int), _, _) (_, (light_count2 : int), _, _) =
      compare light_count2 light_count1
    in
    update_descendent_counts ~threshold t;
    let rec loop node acc =
      let acc = Node.fold_children loop node acc in
      if Node.is_heavy ~threshold node then
        List.merge cmp [Node.output node] acc
      else
        acc
    in
    Node.fold_children loop t.root [], t.count

end


module Location = struct

  type t = location_code

  let hash (x : t) =
    ((x :> int) * 984372984721) asr 17

  let equal (x : t) (y : t) =
    Int.equal (x :> int) (y :> int)

  let compare (x : t) (y : t) =
    Int.compare (x :> int) (y :> int)

  let dummy = (Obj.magic (-1L : int64) : t)

  let print ppf i =
    Format.fprintf ppf "%x" (i : t :> int)

end

module Loc_hitters = Substring_heavy_hitters(Location)

let wordsize = 8.  (* FIXME: store this in the trace *)

let print_bytes ppf = function
  | n when n < 100. ->
    Format.fprintf ppf "%4.0f B" n
  | n when n < 100. *. 1024. ->
    Format.fprintf ppf "%4.1f kB" (n /. 1024.)
  | n when n < 100. *. 1024. *. 1024. ->
    Format.fprintf ppf "%4.1f MB" (n /. 1024. /. 1024.)
  | n when n < 100. *. 1024. *. 1024. *. 1024. ->
    Format.fprintf ppf "%4.1f GB" (n /. 1024. /. 1024. /. 1024.)
  | n ->
    Format.fprintf ppf "%4.1f TB" (n /. 1024. /. 1024. /. 1024. /. 1024.)

let print_location ppf {filename; line; start_char; end_char; defname} =
  Format.fprintf ppf
    "%s (%s:%d:%d-%d)" defname filename line start_char end_char

let print_locations ppf locations =
  Format.pp_print_list ~pp_sep:Format.pp_print_space print_location
    ppf locations

let print_loc_code trace ppf loc =
  print_locations ppf (List.rev (Memtrace.lookup_location trace loc))

let print_loc_codes trace ppf locs =
  for i = (Array.length locs) - 1 downto 0 do
    Format.pp_print_space ppf ();
    print_loc_code trace ppf locs.(i)
  done

let print_hitter trace tinfo total ppf (locs, light_count, count, _) =
  let freq = float_of_int count /. float_of_int total in
  let light_bytes = float_of_int light_count /. tinfo.sample_rate *. wordsize in
  let bytes = float_of_int count /. tinfo.sample_rate *. wordsize in
  Format.fprintf ppf "@[<v 4>%a/%a (%4.1f%%) under%a@]"
    print_bytes light_bytes print_bytes bytes (100. *. freq)
    (print_loc_codes trace) locs

let print_hitters trace tinfo total ppf hitters =
  let pp_sep ppf () =
    Format.pp_print_space ppf ();
    Format.pp_print_space ppf ()
  in
  Format.pp_print_list ~pp_sep
    (print_hitter trace tinfo total) ppf hitters

let print_report trace ppf (hitters, total) =
  let tinfo = trace_info trace in
  Format.fprintf ppf
    "@[<v 2>@ Trace for %s [%Ld]:@ %d samples of %a allocations@ \
     @[<v 2>@ %a@ @]@ "
    tinfo.executable_name tinfo.pid
    total
    print_bytes (float_of_int total /. tinfo.sample_rate *. wordsize)
    (print_hitters trace tinfo total) hitters

module Seen : sig

  type t

  val empty : t

  val mem : t -> Location.t -> bool

  val add : t -> Location.t -> int -> t

  val size : t -> int

  val pop_until : t -> int -> t

end  = struct

  module Loc_set = Set.Make(Location)

  type t =
    | Nil
    | Cons of
        { set : Loc_set.t;
          size : int;
          depth : int;
          previous : t; }

  let empty = Nil

  let set = function
    | Nil -> Loc_set.empty
    | Cons { set; _ } -> set

  let size = function
    | Nil -> 0
    | Cons { size; _ } -> size

  let mem seen loc =
    Loc_set.mem loc (set seen)

  let add seen loc depth =
    match seen with
    | Nil ->
        let set = Loc_set.singleton loc in
        let size = 1 in
        let previous = seen in
        Cons { set; size; depth; previous }
    | Cons { set; size; _ } ->
        let set = Loc_set.add loc set in
        let size = size + 1 in
        let previous = seen in
        Cons { set; size; depth; previous }

  let rec pop_until seen until =
    match seen with
    | Nil -> seen
    | Cons{depth; previous; _ } ->
        if depth < until then seen
        else pop_until previous until

end

let count ~frequency ~error ~filename =
  let trace = open_trace ~filename in
  let shh = Loc_hitters.create ~error in
  let seen = ref Seen.empty in
  iter_trace trace
    (fun _time ev ->
       match ev with
       | Alloc {obj_id=_; length=_; nsamples; is_major=_;
                backtrace_buffer; backtrace_length; common_prefix} ->
           let rev_trace = ref [] in
           seen := Seen.pop_until !seen common_prefix;
           let common = Seen.size !seen in
           for i = common_prefix to backtrace_length - 1 do
             let loc = backtrace_buffer.(i) in
             if not (Seen.mem !seen loc) then begin
               rev_trace := loc :: !rev_trace;
               seen := Seen.add !seen loc i
             end
           done;
           let extension = Array.of_list (List.rev !rev_trace) in
           Loc_hitters.insert shh ~common_prefix:common
             extension ~count:nsamples
      | Promote _ -> ()
      | Collect _ -> ());
  let results = Loc_hitters.output shh ~frequency in
  Format.printf "%a" (print_report trace) results;
  close_trace trace

let default_frequency = 0.03

let default_error = 0.01

let () =
  if Array.length Sys.argv = 2 then begin
    count ~frequency:default_frequency ~error:default_error ~filename:Sys.argv.(1)
  end else if Array.length Sys.argv = 4 then begin
    match Float.of_string Sys.argv.(2), Float.of_string Sys.argv.(3) with
    | frequency, error -> count ~frequency ~error ~filename:Sys.argv.(1)
    | exception Failure _ ->
        Printf.fprintf stderr "Usage: %s <trace file> [frequency error]\n"
          Sys.executable_name
  end else begin
    Printf.fprintf stderr "Usage: %s <trace file> [frequency error]\n"
      Sys.executable_name
  end
