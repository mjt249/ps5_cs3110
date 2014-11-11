open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type line = string 
  type file = string
  type word  = string
  
  type input = line * file
  type key = word
  type inter = file
  type output = file list

  let name = "index.job"

  module M = Map.Make(String)

  let tag (f:file) table (k:key) = 
    if M.mem k table 
    then table
    else M.add k f table 

  (*
   *Given element and list, appends to the list if the element is not in list.
   *Argument      : l ('a list) and el ('a)
   *Precondition  : l is a list of type equal to type of el
   *Postcondition : Output has no extra el.
   *)
  let append_if_not_exists (l: 'a list) (el: 'a) : 'a list = 
    if (List.mem el l) then
      l
    else
      [el] @ l

  (*
   *Removes duplicates in the list
   *Argument      : l ('a list)
   *Precondition  : l is of type 'a list
   *Postcondition : l has no duplicates and is of type 'a list
   *)
  let remove_dups (l: 'a list) : 'a list =
    List.fold_left append_if_not_exists [] l

  let map input : (key * inter) list Deferred.t =
    let words = AppUtils.split_words (fst input) in
    let map_input = List.fold_left (tag (snd input)) M.empty words in
    return (M.bindings map_input)

  (* Remove duplicates in the inter list since the words might appear
   * multiple times in the same file *)
  let reduce (key, inters) : output Deferred.t =
    return (remove_dups inters)
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>| fun contents ->
      (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    let file_line_pair file_name =
      Reader.file_lines file_name 
      >>= (fun line_list -> Deferred.List.map line_list 
                            (fun line -> return (line, file_name)))

    let read_files file_list =
      match file_list with
      | [] -> failwith "No files in master list."
      | lst -> Deferred.List.map lst file_line_pair

      
    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main args =
      match args with
      | []  -> failwith "No master file provided."
      | [file_list] -> 
          Reader.file_lines file_list
          >>= read_files
          >>| List.flatten
          >>= MR.map_reduce
          >>| output
      | _ -> failwith "Invalid number of arguments."
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

