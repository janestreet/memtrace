(* A generalized suffix tree based on Ukkonen's algorithm
   combined with lossy counting. *)

open Memtrace

module type Char = sig

  include Hashtbl.HashedType

  val dummy : t

  val less_than : t -> t -> bool

  val print : Format.formatter -> t -> unit

  val print_array : Format.formatter -> t array -> unit

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

    val label : t -> X.t array

    module Root : sig

      type node = t

      type t

      val create : unit -> t

      val node : t -> node

      val is_node : t -> node -> bool

    end

    val add_leaf :
      root:Root.t -> parent:t -> array:X.t array -> index:int
      -> key:X.t -> t

    val split_edge :
      root:Root.t -> parent:t -> child:t -> len:int -> t

    val set_suffix : root:Root.t -> t -> suffix:t -> unit

    val add_to_count :
      root:Root.t -> depth:int -> count:int -> t -> unit

    type find_result =
      | Found of t
      | Added of t

    val find_or_add_leaf :
      root:Root.t -> parent:t -> array:X.t array -> index:int -> find_result

    val get_child : root:Root.t -> t -> X.t -> t

    val edge_array : t -> X.t array

    val edge_start : t -> int

    val edge_length : t -> int

    val edge_key : t -> X.t

    val edge_char : t -> int -> X.t

    val has_suffix : t -> bool

    val suffix : t -> t

    val parent : t -> t

    val compress : root:Root.t -> threshold:int -> unit

    val reset_descendents_count : t -> unit

    val update_parents_descendents_counts :
      root:Root.t -> threshold:int -> t -> unit

    val iter_children : root:Root.t -> (t -> unit) -> t -> unit

    val fold_children : root:Root.t -> (t -> 'a -> 'a) -> t -> 'a -> 'a

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
        mutable next_sibling : t;
        mutable first_child : t;
        mutable refcount : int;
        (* [2 * incoming suffix links + 2 * has count + chidren]
           A node should be deleted when this is <= 1 *)
        mutable queue_item : queue_item;
        mutable output : output;
        mutable count : int;
        mutable max_edge_squashed : int;
        mutable max_child_squashed : int; }

    and queue_item =
      { node : t;
        mutable next : queue_item;
        mutable previous: queue_item; }

    and output =
      | No_output
      | Output of
          { mutable descendents_count : int;
            mutable heavy_descendents_count : int; }

    let same t1 t2 = t1 == t2

    let is_removable refcount = refcount = 0
    let is_mergable refcount = refcount = 1
    let is_mergable_or_removable refcount = refcount <= 1

    let dummy_array = [||]

    let rec dummy_queue_item =
      { node = dummy;
        next = dummy_queue_item;
        previous = dummy_queue_item; }

    and dummy =
      let edge_array = dummy_array in
      let edge_start = 0 in
      let edge_len = 0 in
      let edge_key = X.dummy in
      let refcount = 0 in
      let output = No_output in
      let count = 0 in
      let max_child_squashed = 0 in
      let max_edge_squashed = 0 in
      { edge_array; edge_start; edge_len; edge_key;
        parent = dummy; suffix_link = dummy; next_sibling = dummy;
        first_child = dummy; refcount; output;
        queue_item = dummy_queue_item;
        count; max_edge_squashed; max_child_squashed; }

    let is_dummy t = same t dummy
    let is_real t = not (is_dummy t)

    module Queue = struct

      module Item = struct

        type nonrec t = queue_item

        let root =
          { node = dummy;
            next = dummy_queue_item;
            previous = dummy_queue_item }

        let dummy = dummy_queue_item

        let same t1 t2 = t1 == t2

        let is_dummy t = same t dummy
        let is_real t = not (is_dummy t)

        let next item = item.next

        let set_next item ~next =
          item.next <- next

        let set_previous item ~previous =
          if is_real item then begin
            item.previous <- previous
          end

        let fresh ~node ~previous ~next =
          { node; previous; next; }

      end

      type t =
        { mutable fronts : Item.t Array.t;
          mutable max : int; }

      let create () =
        let fronts = [| |] in
        let max = -1 in
        { fronts; max }

      let enlarge t =
        let fronts = t.fronts in
        let old_length = Array.length fronts in
        let new_length = (old_length * 2) + 1 in
        let new_fronts = Array.make new_length dummy_queue_item in
        Array.blit fronts 0 new_fronts 0 old_length;
        t.fronts <- new_fronts

      let fresh_front_sentinel () =
        let node = dummy in
        let previous = dummy_queue_item in
        let next = dummy_queue_item in
        Item.fresh ~node ~previous ~next

      let add t ~depth ~node =
        if depth > t.max then begin
          while depth >= Array.length t.fronts do
            enlarge t
          done;
          t.max <- depth
        end;
        let front = t.fronts.(depth) in
        let item =
          if Item.is_real front then begin
            let previous = front in
            let next = Item.next front in
            let item = Item.fresh ~node ~next ~previous in
            Item.set_previous next ~previous:item;
            Item.set_next previous ~next:item;
            item
          end else begin
            let previous = fresh_front_sentinel () in
            t.fronts.(depth) <- previous;
            let next = Item.dummy in
            let item = Item.fresh ~node ~previous ~next in
            Item.set_next previous ~next:item;
            item
          end
        in
        node.queue_item <- item

      let iter_front front depth f =
        let current = ref (Item.next front) in
        while Item.is_real !current do
          let next_current = !current.next in
          f ~depth !current;
          current := next_current
        done

      let iter t f =
        let fronts = t.fronts in
        for i = t.max downto 0 do
          iter_front fronts.(i) i f
        done

    end

    let label t =
      let rec loop acc t =
        let edge = Array.sub t.edge_array t.edge_start t.edge_len in
        if not (same t t.parent) then loop (edge :: acc) t.parent
        else Array.concat (edge :: acc)
      in
      loop [] t

    module Root = struct

      type node = t

      type t =
        { node : node;
          children : node Tbl.t;
          queue : Queue.t; }

      let create () =
        let edge_array = dummy_array in
        let edge_start = 0 in
        let edge_len = 0 in
        let edge_key = X.dummy in
        let next_sibling = dummy in
        let first_child = dummy in
        let refcount = 0 in
        let queue_item = Queue.Item.root in
        let output = No_output in
        let count = 0 in
        let max_child_squashed = 0 in
        let max_edge_squashed = 0 in
        let rec node =
          { edge_array; edge_start; edge_len; edge_key;
            parent = node; suffix_link = node; next_sibling;
            first_child; refcount; output; queue_item;
            count; max_edge_squashed; max_child_squashed }
        in
        let children = Tbl.create 37 in
        let queue = Queue.create () in
        { node; children; queue }

      let node t = t.node

      let is_node t node =
        same t.node node

      let children t =
        t.children

      let queue t =
        t.queue

    end

    let rec set_child_in_list previous current old_child new_child =
      let next = current.next_sibling in
      if same current old_child then begin
        previous.next_sibling <- new_child;
        new_child.next_sibling <- next
      end else begin
        set_child_in_list current next old_child new_child
      end

    let set_child ~root ~parent ~key ~old_child ~new_child =
      if Root.is_node root parent then begin
        Tbl.replace (Root.children root) key new_child
      end else begin
        let first_child = parent.first_child in
        let second_child = first_child.next_sibling in
        if same first_child old_child then begin
          parent.first_child <- new_child;
          new_child.next_sibling <- second_child
        end else begin
          set_child_in_list first_child second_child old_child new_child
        end
      end

    let rec add_child_to_list ~key ~child previous current =
      let current_key = current.edge_key in
      if X.less_than key current_key then begin
        child.next_sibling <- current;
        previous.next_sibling <- child
      end else begin
        add_child_to_list ~key ~child
          current current.next_sibling
      end

    let add_child ~root ~parent ~key ~child =
      if Root.is_node root parent then begin
        Tbl.add (Root.children root) key child
      end else begin
        parent.refcount <- parent.refcount + 1;
        let first_child = parent.first_child in
        let first_key = first_child.edge_key in
        if X.less_than key first_key then begin
          child.next_sibling <- first_child;
          parent.first_child <- child;
        end else begin
          add_child_to_list ~key ~child
            first_child first_child.next_sibling
        end
      end

    let rec remove_from_child_list previous current child =
      let next = current.next_sibling in
      if same current child then begin
        previous.next_sibling <- next;
      end else begin
        remove_from_child_list current next child;
      end

    let remove_child ~root ~parent ~child =
      if Root.is_node root parent then begin
        let key = child.edge_key in
        Tbl.remove (Root.children root) key;
        max_int
      end else begin
        let first_child = parent.first_child in
        let second_child = first_child.next_sibling in
        let refcount = parent.refcount - 1 in
        parent.refcount <- refcount;
        if same first_child child then begin
          parent.first_child <- second_child
        end else begin
          remove_from_child_list first_child second_child child
        end;
        refcount
      end

    let set_suffix ~root t ~suffix =
      t.suffix_link <- suffix;
      if not (Root.is_node root suffix) then
        suffix.refcount <- suffix.refcount + 2

    let remove_incoming ~root t =
      if Root.is_node root t then max_int
      else begin
        let refcount = t.refcount - 2 in
        t.refcount <- refcount;
        refcount
      end

    let fresh_leaf ~parent ~array ~index ~key =
      let edge_array = array in
      let edge_start = index in
      let edge_len = (Array.length array) - index in
      let edge_key = key in
      let suffix_link = dummy in
      let next_sibling = dummy in
      let first_child = dummy in
      let queue_item = dummy_queue_item in
      let output = No_output in
      let count = 0 in
      let refcount = 0 in
      let max_edge_squashed = parent.max_child_squashed in
      let max_child_squashed = parent.max_child_squashed in
      { edge_array; edge_start; edge_len; edge_key;
        parent; suffix_link; next_sibling;
        first_child; refcount; output; queue_item;
        count; max_edge_squashed; max_child_squashed; }

    let add_leaf ~root ~parent ~array ~index ~key =
      let node = fresh_leaf ~parent ~array ~index ~key in
      add_child ~root ~parent ~key ~child:node;
      node

    let split_edge ~root ~parent ~child ~len =
      if (len = 0) then parent
      else begin
        let edge_array = child.edge_array in
        let edge_start = child.edge_start in
        let edge_key = child.edge_key in
        let child_key = edge_array.(edge_start + len) in
        let new_node =
          let edge_len = len in
          let suffix_link = dummy in
          let next_sibling = dummy in
          let refcount = 1 in
          let first_child = child in
          let queue_item = dummy_queue_item in
          let output = No_output in
          let count = 0 in
          let max_edge_squashed = child.max_edge_squashed in
          let max_child_squashed = child.max_edge_squashed in
          { edge_array; edge_start; edge_len; edge_key;
            parent; suffix_link; next_sibling;
            first_child; refcount; output; queue_item;
            count; max_edge_squashed; max_child_squashed}
        in
        set_child ~root ~parent ~key:edge_key ~old_child:child ~new_child:new_node;
        child.edge_start <- edge_start + len;
        child.edge_len <- child.edge_len - len;
        child.edge_key <- child_key;
        child.parent <- new_node;
        child.next_sibling <- dummy;
        new_node
      end

    let merge_child ~root ~parent t =
      let child = t.first_child in
      let key = t.edge_key in
      let edge_len = t.edge_len in
      let child_edge_start = child.edge_start in
      child.edge_key <- key;
      if child_edge_start >= edge_len then begin
        child.edge_start <- child_edge_start - edge_len;
      end else begin
        let edge_array = t.edge_array in
        let edge_start = t.edge_start in
        let common_prefix = edge_start + (edge_len - child_edge_start) in
        let common =
          if Array.length edge_array = common_prefix then edge_array
          else Array.sub edge_array 0 common_prefix
        in
        let array = Array.append common child.edge_array in
        child.edge_array <- array;
        child.edge_start <- t.edge_start;
      end;
      child.edge_len <- edge_len + child.edge_len;
      let max_edge_squashed = t.max_edge_squashed in
      let child_max_edge_squashed = child.max_edge_squashed in
      if max_edge_squashed > child_max_edge_squashed then
        child.max_edge_squashed <- max_edge_squashed;
      child.parent <- parent;
      set_child ~root ~parent ~key ~old_child:t ~new_child:child

    type find_result =
      | Found of t
      | Added of t

    let rec find_or_add_leaf_in_list ~parent ~array ~index ~key
              previous current =
      let current_key = current.edge_key in
      if X.less_than key current_key then begin
        let child = fresh_leaf ~parent ~array ~index ~key in
        child.next_sibling <- current;
        previous.next_sibling <- child;
        parent.refcount <- parent.refcount + 1;
        Added child
      end else if X.equal key current_key then begin
        Found current
      end else begin
        find_or_add_leaf_in_list ~parent ~array ~index ~key
          current current.next_sibling
      end

    let find_or_add_leaf ~root ~parent ~array ~index =
      let key = array.(index) in
      if Root.is_node root parent then begin
        let children = Root.children root in
        match Tbl.find_opt children key with
        | Some child -> Found child
        | None ->
            let leaf = fresh_leaf ~parent ~array ~index ~key in
            Tbl.add children key leaf;
            Added leaf
      end else begin
        let first_child = parent.first_child in
        let first_key = first_child.edge_key in
        if X.less_than key first_key then begin
          let leaf = fresh_leaf ~parent ~array ~index ~key in
          leaf.next_sibling <- first_child;
          parent.first_child <- leaf;
          parent.refcount <- parent.refcount + 1;
          Added leaf
        end else if X.equal key first_key then begin
          Found first_child
        end else begin
          find_or_add_leaf_in_list
            ~parent ~array ~index ~key
            first_child first_child.next_sibling
        end
      end

    let rec get_child_in_list current count char =
      if X.equal current.edge_key char then begin
        current
      end
      else get_child_in_list current.next_sibling (count + 1) char

    let get_child ~root t char =
      if Root.is_node root t then begin
        match Tbl.find (Root.children root) char with
        | child -> child
        | exception Not_found -> failwith "get_child: No such child"
      end else begin
        get_child_in_list t.first_child 1 char
      end

    let edge_array t = t.edge_array

    let edge_start t = t.edge_start

    let edge_length t = t.edge_len

    let edge_key t = t.edge_key

    let edge_char t i =
      if i = 0 then t.edge_key
      else t.edge_array.(t.edge_start + i)

    let has_suffix t = is_real t.suffix_link

    let suffix t = t.suffix_link

    let parent t = t.parent

    let count t = t.count

    let reset_descendents_count t =
      t.output <- No_output

    let add_to_descendents_count t diff =
      match t.output with
      | No_output ->
          let descendents_count = diff in
          let heavy_descendents_count = 0 in
          let output =
            Output { descendents_count; heavy_descendents_count }
          in
          t.output <- output
      | Output o ->
          o.descendents_count <- o.descendents_count + diff

    let add_to_heavy_descendents_count t diff =
      match t.output with
      | No_output ->
          let descendents_count = 0 in
          let heavy_descendents_count = diff in
          let output =
            Output { descendents_count; heavy_descendents_count }
          in
          t.output <- output
      | Output o ->
          o.heavy_descendents_count <- o.heavy_descendents_count + diff

    let descendents_count t =
      match t.output with
      | No_output -> 0
      | Output { descendents_count; _ } -> descendents_count

    let heavy_descendents_count t =
      match t.output with
      | No_output -> 0
      | Output { heavy_descendents_count; _ } -> heavy_descendents_count

    let update_parents_descendents_counts ~root ~threshold t =
      if not (Root.is_node root t) then begin
        let heavy_descendents_count = heavy_descendents_count t in
        let descendents_count = descendents_count t in
        let total = count t + descendents_count in
        let light_total = total - heavy_descendents_count in
        let delta = t.max_edge_squashed in
        let heavy_total =
          if light_total + delta > threshold then total
          else heavy_descendents_count
        in
        let parent = t.parent in
        let suffix = t.suffix_link in
        let grand_parent = t.parent.suffix_link in
        if is_real parent then begin
          add_to_descendents_count parent total;
          add_to_heavy_descendents_count parent heavy_total
        end;
        if is_real suffix then begin
          add_to_descendents_count suffix total;
          add_to_heavy_descendents_count suffix heavy_total
        end;
        if is_real grand_parent then begin
          add_to_descendents_count grand_parent (-total);
          add_to_heavy_descendents_count grand_parent (-heavy_total)
        end
      end

    let is_heavy ~threshold t =
      let heavy_descendents_count = heavy_descendents_count t in
      let descendents_count = descendents_count t in
      let total = count t + descendents_count in
      let light_total = total - heavy_descendents_count in
      let delta = t.max_edge_squashed in
      light_total + delta > threshold

    let output t =
      let heavy_descendents_count = heavy_descendents_count t in
      let descendents_count = descendents_count t in
      let total = count t + descendents_count in
      let light_total = total - heavy_descendents_count in
      let delta = t.max_edge_squashed in
      (label t, light_total, total, total + delta)

    let iter_over_child_list f current =
      let current = ref current in
      while !current != dummy do
        let child = !current in
        f child;
        current := child.next_sibling
      done

    let iter_children ~root f t =
      if Root.is_node root t then begin
        Tbl.iter (fun _ n -> f n) (Root.children root)
      end else begin
        iter_over_child_list f t.first_child
      end

    let fold_over_child_list f current acc =
      let acc = ref acc in
      let current = ref current in
      while !current != dummy do
        let child = !current in
        acc := f child !acc;
        current := child.next_sibling
      done;
      !acc

    let fold_children ~root f t acc =
      if Root.is_node root t then begin
        Tbl.fold (fun _ n -> f n) (Root.children root) acc
      end else begin
        fold_over_child_list f t.first_child acc
      end

    let add_to_count t ~count =
      t.count <- t.count + count

    let register_for_compression ~queue ~depth t =
      if Queue.Item.is_dummy t.queue_item then begin
        Queue.add queue ~depth ~node:t;
      end

    let add_squashed_child t ~upper_bound =
      if upper_bound > t.max_child_squashed then begin
        t.max_child_squashed <- upper_bound
      end

    let add_squashed_edge t ~upper_bound =
      if upper_bound > t.max_edge_squashed then begin
        t.max_edge_squashed <- upper_bound
      end

    let rec squash ~root ~queue ~threshold
              ~depth ~count ~upper_bound ~refcount t =
      t.count <- 0;
      let parent = t.parent in
      let suffix = t.suffix_link in
      let grand_parent = t.parent.suffix_link in
      add_squashed_edge t ~upper_bound;
      add_squashed_child parent ~upper_bound;
      let parent_depth = depth - t.edge_len in
      let suffix_depth = depth - 1 in
      let grand_parent_count = 0 - count in
      add_to_count grand_parent ~count:grand_parent_count;
      add_to_count parent ~count;
      add_to_count suffix ~count;
      if is_removable refcount then begin
        let parent_refcount = remove_child ~root ~parent ~child:t in
        if is_mergable_or_removable parent_refcount then begin
          register_for_compression ~queue ~depth:parent_depth parent
        end;
      end else if is_mergable refcount then begin
        merge_child ~root ~parent t
      end;
      if is_mergable_or_removable refcount then begin
        let suffix_refcount = remove_incoming ~root suffix in
        if is_removable suffix_refcount then begin
          let count = suffix.count in
          let delta = suffix.max_edge_squashed in
          let upper_bound = count + delta in
          if upper_bound < threshold then begin
            squash ~root ~queue ~threshold ~depth:suffix_depth
              ~count ~upper_bound ~refcount:suffix_refcount suffix
          end else begin
            register_for_compression ~queue ~depth:suffix_depth suffix
          end
        end else if is_mergable suffix_refcount then begin
          register_for_compression ~queue ~depth:suffix_depth suffix
        end
      end

    let maybe_squash_item ~root ~queue ~threshold ~depth item =
        let node = item.node in
        let count = node.count in
        let delta = node.max_edge_squashed in
        let upper_bound = count + delta in
        if upper_bound < threshold then begin
          let next = item.next in
          let previous = item.previous in
          Queue.Item.set_previous next ~previous;
          Queue.Item.set_next previous ~next;
          node.queue_item <- Queue.Item.dummy;
          let refcount = node.refcount in
          squash ~root ~queue ~threshold ~depth
            ~count ~upper_bound ~refcount node
        end

    let compress ~root ~threshold =
      let queue = Root.queue root in
      Queue.iter queue (maybe_squash_item ~root ~queue ~threshold)

    let add_to_count ~root ~depth ~count t =
      let queue = Root.queue root in
      add_to_count ~count t;
      register_for_compression ~queue ~depth t

  end

  module Cursor : sig

    type t

    val create : at:Node.t -> t

    val goto : t -> Node.t -> unit

    val retract : t -> distance:int -> unit

    type find_result =
      | Found
      | Added of { parent : Node.t; leaf : Node.t; }

    val find_or_add_leaf :
      root:Node.Root.t -> t -> array:X.t array -> index:int -> find_result

    val split_at : root:Node.Root.t -> t -> Node.t

    val goto_suffix : root:Node.Root.t -> t -> Node.t -> unit

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

    type find_result =
      | Found
      | Added of { parent : Node.t; leaf : Node.t; }

    let find_or_add_leaf ~root t ~array ~index =
      let len = t.len in
      let parent = t.parent in
      if len = 0 then begin
        match Node.find_or_add_leaf ~root ~parent ~array ~index with
        | Found child ->
            t.child <- child;
            extend t;
            Found
        | Added leaf -> Added { parent; leaf }
      end else begin
        let char = array.(index) in
        let next_char = Node.edge_char t.child len in
        if X.equal char next_char then begin
          extend t;
          Found
        end else begin
          let child = t.child in
          let parent = Node.split_edge ~root ~parent ~child ~len in
          let leaf =
            Node.add_leaf ~root ~parent ~array ~index ~key:char
          in
          goto t parent;
          Added { parent; leaf }
        end
      end

    let split_at ~root t =
      let len = t.len in
      if len = 0 then t.parent
      else begin
        let node = Node.split_edge ~root ~parent:t.parent ~child:t.child ~len in
        goto t node;
        node
      end

    let rec rescan ~root t ~array ~start ~len =
      if len <> 0 then begin
        if t.len = 0 then begin
          let char = array.(start) in
          let child = Node.get_child ~root t.parent char in
          t.child <- child
        end;
        let diff = extend_n t len in
        let start = start + diff in
        let len = len - diff in
        rescan ~root t ~array ~start ~len
      end

    let rescan1 ~root t ~key =
      if t.len = 0 then begin
        let child = Node.get_child ~root t.parent key in
        t.child <- child
      end;
      extend t

    let rec goto_suffix ~root t node =
      if Node.Root.is_node root node then begin
        goto t node
      end else if Node.has_suffix node then begin
        goto t (Node.suffix node)
      end else begin
        let parent = Node.parent node in
        let len = Node.edge_length node in
        if len = 1 then begin
          if Node.Root.is_node root parent then begin
            goto t parent
          end else begin
            let key = Node.edge_key node in
            goto_suffix ~root t parent;
            rescan1 ~root t ~key
          end
        end else begin
          let array = Node.edge_array node in
          let start = Node.edge_start node in
          if Node.Root.is_node root parent then begin
            goto t parent;
            let start = start + 1 in
            let len = len - 1 in
            rescan ~root t ~array ~start ~len
          end else begin
            goto_suffix ~root t parent;
            rescan ~root t ~array ~start ~len
          end
        end
      end

  end

  type state =
    | Uncompressed
    | Compressed of X.t array

  type t =
    { root : Node.Root.t;
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
    let root = Node.Root.create () in
    let max_length = 0 in
    let count = 0 in
    let bucket_size = Float.to_int (Float.ceil (1.0 /. error)) in
    let current_bucket = 0 in
    let remaining_in_current_bucket = bucket_size in
    let active = Cursor.create ~at:(Node.Root.node root) in
    let previous_length = 0 in
    let state = Uncompressed in
    { root; max_length; count;
      bucket_size; current_bucket; remaining_in_current_bucket;
      active; previous_length; state }

  let update_descendent_counts ~threshold t =
    let root = t.root in
    let nodes : Node.t list array = Array.make (t.max_length + 1) [] in
    let rec loop depth node =
      Node.reset_descendents_count node;
      let depth = depth + Node.edge_length node in
      nodes.(depth) <- node :: nodes.(depth);
      Node.iter_children ~root (loop depth) node
    in
    loop 0 (Node.Root.node root);
    for i = t.max_length downto 0 do
      List.iter
        (Node.update_parents_descendents_counts ~root ~threshold)
        nodes.(i)
    done

  let rec ensure_suffix ~root cursor t =
    if not (Node.has_suffix t) then begin
      Cursor.goto_suffix ~root cursor t;
      let suffix = Cursor.split_at ~root cursor in
      ensure_suffix ~root cursor suffix;
      Node.set_suffix ~root t ~suffix
    end

  let insert t ~common_prefix array ~count =
    let len = Array.length array in
    let total_len = common_prefix + len in
    if total_len > t.max_length then t.max_length <- total_len;
    t.count <- t.count + count;
    let root = t.root in
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
    let rec loop array len base root active index j =
      if index >= len then begin
        let destination = Cursor.split_at ~root active in
        ensure_suffix ~root active destination;
        destination
      end else begin
        loop_inner array len base root active index j
      end
    and loop_inner array len base root active index j =
      if j > base + index then begin
        loop array len base root active (index + 1) j
      end else begin
        match Cursor.find_or_add_leaf ~root active ~array ~index with
        | Found ->
            loop array len base root active (index + 1) j
        | Added { parent; leaf } ->
            Cursor.goto_suffix ~root active parent;
            let leaf_suffix =
              if Node.has_suffix parent then begin
                loop_inner array len base root active index (j + 1)
              end else begin
                let suffix = Cursor.split_at ~root active in
                let leaf_suffix =
                  loop_inner array len base root active index (j + 1)
                in
                Node.set_suffix ~root parent ~suffix;
                leaf_suffix
              end
            in
            Node.set_suffix ~root leaf ~suffix:leaf_suffix;
            leaf
      end
    in
    let destination = loop array len base root active 0 0 in
    Node.add_to_count ~root ~depth:total_len ~count destination;
    let remaining = t.remaining_in_current_bucket - 1 in
    if remaining <= 0 then begin
      t.current_bucket <- t.current_bucket + 1;
      t.remaining_in_current_bucket <- t.bucket_size;
      let destination_label = Node.label destination in
      Cursor.goto active (Node.Root.node t.root);
      let threshold = t.current_bucket in
      Node.compress ~root ~threshold;
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
    let root = t.root in
    update_descendent_counts ~threshold t;
    let rec loop node acc =
      let acc = Node.fold_children ~root loop node acc in
      if Node.is_heavy ~threshold node then
        List.merge cmp [Node.output node] acc
      else
        acc
    in
    let node = Node.Root.node root in
    Node.fold_children ~root loop node [], t.count

end


module Location = struct

  type t = location_code

  let hash (x : t) =
    ((x :> int) * 984372984721) asr 17

  let equal (x : t) (y : t) =
    Int.equal (x :> int) (y :> int)

  let compare (x : t) (y : t) =
    Int.compare (x :> int) (y :> int)

  let dummy = (Obj.magic (max_int : int) : t)

  let less_than (x : t) (y : t) = x < y

  let print ppf i =
    Format.fprintf ppf "%x" (i : t :> int)

  let print_array ppf a =
    for i = 0 to Array.length a - 1 do
      print ppf a.(i);
      Format.fprintf ppf "; "
    done

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
