open Async.Std

(*Runs the two functions concurrently when the suppled deferred computation
  becomes determined, passing those functions the value determined by the
  original deferred, and ignores the deferred values from the two funtions.*)
let fork (d :'a Deferred.t) (f1: 'a -> 'b Deferred.t) 
  (f2 : 'a -> 'c Deferred.t) : unit =
  ignore (d >>= fun v ->
  	return ((f1 v), (f2 v))) 
 
 (*return the result of mapping the list l through the function f.
   should apply f concurrently—not sequentially—to each element of l.*)   
let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t): 'b list Deferred.t =
  let fold_help acc a=
    (f a) >>= fun v -> 
    acc >>= fun lst ->
      return (v::lst) in
  List.fold_left (fold_help) (return []) l 
  >>| List.rev

