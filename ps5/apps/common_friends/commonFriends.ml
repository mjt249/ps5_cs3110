open Async.Std

module Job = struct
  type input  = string * string list
  type key    = string * string
  type inter  = string list
  type output = string list

  let rec find_other_nodes friendlist node = 
    match friendlist with
    | [] -> []
    | hd::tl when hd <> node -> hd::(find_other_nodes tl node)
    | hd::tl -> find_other_nodes tl node

  let rec compute_pairs name friendlist =
    match friendlist with
    | [] -> []
    | hd::tl when compare name hd = -1 -> 
       ((name, hd), find_other_nodes friendlist hd)::(compute_pairs name tl)
    | hd::tl -> ((hd, name), find_other_nodes friendlist hd)::(compute_pairs name tl)

  let name = "friends.job"
  (* string * string list -> (key * inter) list Deferred.t  
     gets passed name of root node and list of all connected nodes 
     should I compute all pairs?
   *)
  let map (name, friendlist) =
    let sorted_list = List.fast_sort compare friendlist in 
    return (compute_pairs name sorted_list)

  let rec reduce_helper (pair, friendlists) = 
    (* inter list should only ever have length 2 *)
    match friendlists with
    | [] -> []
    | []::[] -> []
    | [hd::tl]::[] -> []
    | []::[hd::tl] -> []
    | [hd::tl]::[x::xs]::[] -> if compare hd x = 0 then hd::(reduce_helper (pair, [tl]::[[xs]]))
			   else if compare hd x = -1 then reduce_helper (pair, [tl]::[[x::xs]])
			   else reduce_helper (pair, [hd::tl]::[[xs]])
    | _ -> failwith "Invalid list size"
  (* (key * inter list) -> string list Deferred.t 
     should take in lists of pairs and see if they match any other pair or something
   *)
  let reduce (pair, friendlists) =
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
  | []    -> failwith "No graph files provided."
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
