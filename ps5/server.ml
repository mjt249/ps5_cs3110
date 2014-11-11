open Core.Std
open Async.Std

let run () =
  let handler addr r w = 
    Pipe.transfer_id (Reader.pipe r) (Writer.pipe w) in 
  let _ = 
    Tcp.Server.create
      ~on_handler_error:`Raise
      (Tcp.on_port 3110)
      handler in 
  Deferred.never ()

let () =
  don't_wait_for (run () >>= fun () -> exit 0);
  ignore (Scheduler.go ())
