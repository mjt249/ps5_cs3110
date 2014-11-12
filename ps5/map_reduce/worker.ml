open Async.Std
open Protocol

module Make (Job : MapReduce.Job) = struct

  module JobRequest = WorkerRequest(Job)
  module JobResponse = WorkerResponse(Job)

  (* see .mli *)
  let run r w =
    JobRequest.receive r >>= fun contents -> 
      match contents with 
      | `Eof -> failwith "Pipe is closed"
      | `Ok (JobRequest.MapRequest input) -> 
	 let mapped = Job.map input in 
      | `Ok (JobRequest.ReduceRequest (key, inter)) -> return ()

end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


