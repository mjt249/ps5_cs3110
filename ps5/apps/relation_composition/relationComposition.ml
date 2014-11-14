open Async.Std
open AppUtils

type relation = R | S

module Job = struct
  type input  = relation * string * string
  type key    = string
  type inter  = relation * string
  type output = (string * string) list

  let name = "composition.job"
	       
  (*
   * Takes two lists and an empty list. Returns the cartesian product of the two 
   * nonempty lists.
   * Arguments     : l1, an 'a list, l2, an 'a list, and acc which is also an 'a list
   * Preconditions : acc should be []
   * Postconditions: output is a list of 'a * 'a tuples representing the cartesian
   *                 product of l1 and l2
   *)
  let rec cartesian_product l1 l2 acc =
    match l1 with
    | [] -> acc
    | h::t -> cartesian_product t l2 ((List.map (fun y -> (h,y)) l2) @ acc)
  
  (*
   * Takes a list of relation * string tuples and two empty lists. Returns a 
   * string list * string list tuple representing all string values paired with S on 
   * the right side and all string values paired with R on the left.
   * Arguments     : an inter list vs, and two string lists, lr and ls
   * Preconditions : lr and ls should be []
   * Postconditions: Output is a string list * string list representing all string 
   * values paired with S on the right side and all string values paired with R on the left.
   *)
  let rec partition_relation vs lr ls =
    match vs with
    | [] -> (lr, ls)
    | (R,f)::t -> partition_relation t (f::lr) ls 
    | (S,s)::t -> partition_relation t lr (s::ls)
        (*
	match h with
        | (R, f) -> partition_relation t (f::lr) ls 
        | (S, s) -> partition_relation t lr (s::ls)
         *)
  let map (r, x, y) : (key * inter) list Deferred.t =
    match r with
    | R -> return ([(y, (r, x))]) 
    | S -> return ([(x, (r, y))])

  let reduce ((k: key), (vs: inter list)) : output Deferred.t =
    let (lr, ls) = partition_relation vs [] [] in
    return (cartesian_product lr ls [])
end

let () = MapReduce.register_job (module Job)

let read_line (line: string) : (string * string) =
  match Str.split (Str.regexp ",") line with
    | [domain; range] -> (String.trim domain, String.trim range)
    | _ -> failwith "Malformed input in relation file."

let read_file (r: relation) (file: string) : (relation * string * string) list Deferred.t =
      Reader.file_lines file            >>= fun lines  ->
      return (List.map read_line lines) >>= fun result ->
      return (List.map (fun (domain, range) -> (r, domain, range)) result)

module App = struct
  let name = "composition"

  let clean_and_print vs =
    List.map snd vs   |>
    List.flatten      |>
    List.sort compare |>
    List.iter (fun (a, b) -> printf "(%s, %s)\n" a b)

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (* You may assume that main is called with two valid relation files. You do
       not need to handle malformed input. For example relation files, see the
       data directory. *)
    let main args =
      match args with
      | [rfile; sfile] -> begin
          read_file R rfile >>= fun r ->
          read_file S sfile >>= fun s ->
          return (r @ s)
          >>= MR.map_reduce
          >>| clean_and_print
      end
      | _ -> failwith "Incorrect number of input files. Please provide two files."
  end
end

let () = MapReduce.register_app (module App)
