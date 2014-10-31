open AQueue
open Async.Std

let tester () =
	let que = create() in
	let () = push que 1 in
	let va_d : 'a Deferred.t = pop que in
	let va_o = Deferred.peek va_d in

   print_endline (string_of_bool(va_d = 1))

let _ = tester ()

let _ =
  Scheduler.go ()