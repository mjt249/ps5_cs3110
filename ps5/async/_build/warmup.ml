open Async.Std

let fork d f1 f2 =
  match d >>= fun x -> return (f1 x, f2 x)  with
  | _ -> ()

let deferred_map l f =
  List.fold_right (fun x acc -> f x >>= (fun h -> acc >>= (fun t -> return (h::t)))) l (return [])
