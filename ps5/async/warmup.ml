open Async.Std

let fork (d :'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2 : 'a -> 'c Deferred.t) : unit =
  ignore (d >>= fun v ->
    return ((f1 v), (f2 v))) 
    
let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t): 'b list Deferred.t =
  let fold_help acc a=
    (f a) >>= fun v -> 
    acc >>= fun lst ->
      return (v::lst) in
  List.fold_left (fold_help) (return []) l 
  >>| List.rev
