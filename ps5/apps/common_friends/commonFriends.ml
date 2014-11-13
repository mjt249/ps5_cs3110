open Async.Std
module Job = struct
  type input = string * string list
  type key = string * string
  type inter = string list
  type output = string list
  (*
   * Takes a list of nodes and a particular node. Returns the original list without
   * all elements matching the input node.
   * Args : a string list, friendlist, and a node of type string.
   * Preconditions : function must typecheck
   * Postconditions: Output is a list of type inter matching friendlist not containing node
   *)
  let rec find_other_nodes (friendlist: string list) (node: string) : inter =
    match friendlist with
    | [] -> []
    | hd::tl when hd <> node -> hd::(find_other_nodes tl node)
    | hd::tl -> find_other_nodes tl node
  (*
   * Takes a tuple of a string, and two identical string lists. Outputs a list of key, inter
   * tuples corresponding to a key of the string matched with every element in the string
   * list with each combination's inter being a list of all the other strings in the string
   * list.
   * Args : a string, name, and two identical string lists, friendlist_complete
   * and friendlist_partial
   * Preconditions : friendlist_partial and friendlist_complete must be identical
   * Postconditions: Output is a list of key inter tuples. Each tuple contains name matched
   * with one element from one of the friendlists and the inter is a list of
   * the rest of the elements in friendlist
   *)
  let rec compute_pairs (name, friendlist_complete, friendlist_partial) : (key * inter) list =
    match friendlist_partial with
    | [] -> []
    | hd::tl when compare name hd = -1 ->
       ((name, hd), (find_other_nodes friendlist_complete hd))::(
	compute_pairs (name, friendlist_complete, tl))
    | hd::tl -> ((hd, name), find_other_nodes friendlist_complete hd)::(
	       compute_pairs (name, friendlist_complete, tl))
  let name = "friends.job"
  let map (name, friendlist): (key * inter) list Deferred.t =
    let sorted_list = List.fast_sort compare friendlist in
    return (compute_pairs (name, sorted_list, sorted_list))
  (* Performs the exact same as reduce, but output is of type output, not output Deferred.t *)
  let rec reduce_helper (pair, (friendlists: string list list)) : output=
    match friendlists with
    | [] -> []
    | []::[] -> []
    | hd::[[]] -> []
    | []::hd::[] -> []
    | (hd::tl)::(x::xs)::[] -> if compare hd x = 0 then hd::(reduce_helper (pair, (tl::[xs])))
			       else (if compare hd x = -1 then reduce_helper (pair, (tl::[x::xs]))
				     else reduce_helper (pair, ((hd::tl)::[xs])))
    | _ -> failwith "Invalid list size"
  let reduce ((pair: key), (friendlists: inter list)) : output Deferred.t =
    return (reduce_helper (pair, friendlists))
end
let () = MapReduce.register_job (module Job)
let read_line (line:string) :(string * (string list)) =
  match Str.split (Str.regexp ":") line with
  | [person; friends] -> begin
			 let friends = Str.split (Str.regexp ",") friends in
			 let trimmed = List.map String.trim friends in
			 (person, trimmed)
		       end
  | _ -> failwith "Malformed input in graph file."
let read_files (files: string list) : ((string * (string list)) list) Deferred.t =
  match files with
  | [] -> failwith "No graph files provided."
  | files -> begin
	     Deferred.List.map files Reader.file_lines
	     >>| List.flatten
	     >>| List.map read_line
	   end
module App = struct
  let name = "friends"
  let print common_friends =
    let print_friends ((a, b), friends) =
      printf "(%s, %s): %s\n" a b (String.concat ", " friends)
    in
    List.iter print_friends common_friends
  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)
    (* You may assume that main is called with a single, valid graph file. You
do not need to handle malformed input. For example graph files, see the
data directory. *)
    let main args =
      read_files args
      >>= MR.map_reduce
      (* replace this failwith with print once you've figured out the key and
inter types*)
      >>| fun x -> print x
  end
end
let () = MapReduce.register_app (module App)
