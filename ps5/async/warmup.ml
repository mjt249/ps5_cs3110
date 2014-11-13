open Async.Std

let fork (d :'a Deferred.t) (f1: 'a -> 'b Deferred.t) (f2 : 'a -> 'c Deferred.t) : unit =
  let res = 
  d
  >>= (fun x -> return ((f1 x), (f2 x)))
  >>= (fun _ -> return ())
  in
  don't_wait_for res
    
let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t): 'b list Deferred.t =
  let fold_help acc a=
    (f a) >>= fun v -> 
    acc >>= fun lst ->
      return (v::lst) in
  List.fold_left (fold_help) (return []) l 
  >>| List.rev
