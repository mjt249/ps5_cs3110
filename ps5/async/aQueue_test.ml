open AQueue
open Assertions
open Async.Std




let tester () =

  let q1 = create() in
  let v1 = pop q1 in
  let () = push q1 3 in
  let () = push q1 2 in

  v1 >>= fun the_el ->
  print_endline (string_of_bool(the_el=3));
  return ()

  let q2 = create() in
  let () = push q2 1 in
  let () = push q2 2 in
  let v2 = pop q2 in

  v2 >>= fun the_el ->
  print_endline (string_of_bool(the_el=1));
  return ()

let _ = tester ()

let _ =
  Scheduler.go ()