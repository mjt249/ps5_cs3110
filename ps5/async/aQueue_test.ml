open Async.Std
open Warmup
open AQueue

let sec = Core.Std.sec

(* Run fork *)
let queue_test () =
  print_endline "Queue should print value after unblocking";
  
  let q = create ()  in
  fork (after (sec 1.0))
       (fun _ -> (pop q) >>= (fun x -> print_endline @@ x ^ " got popped."; return ()))
       (fun _ -> (upon (after (sec 5.0)) (fun () -> push q "1"; ())); return ());
  return ()

(* Run the examples *)
let _ = queue_test ()

(** Start the async scheduler *)
let _ =
  Scheduler.go ()
