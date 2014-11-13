open Async.Std

let fork d f1 f2 : unit =
  let res = 
  d
  >>= (fun x -> return ((f1 x), (f2 x)))
  >>= (fun _ -> return ())
  in
  don't_wait_for res

let all (l:('a Deferred.t) list) : ('a list) Deferred.t = 
  List.fold_right
    (fun x acc -> 
      x >>= (fun h -> 
      acc >>= (fun t -> 
      return (h::t))))
    l (return [])

let deferred_map l f =
  all (List.fold_right (fun x acc -> (f x)::acc) l [])  
