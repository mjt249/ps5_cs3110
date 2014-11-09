open Async.Std

module Make (Job : MapReduce.Job) = struct


  module JobResponse = Protocol.WorkerResponse(Job)
  module JobRequest = Protocol.WorkerRequest(Job)

  (* see .mli *)
  let rec run (r : Reader.t) (w : Writer.t) : unit Deferred.t =
    JobRequest.receive r >>= (fun res -> 
      match res with
      | `Eof -> return() (* failwith "Error, pipe is closed." *)
      | `Ok (JobRequest.MapRequest input) -> 
          let work = Job.map input in
          try_with (fun () -> work) 
          >>= (function
          | Core.Std.Ok determined_work -> 
              let response  = JobResponse.(MapResult determined_work) in
              JobResponse.send w response;
              run r w
          | Core.Std.Error ex -> 
              let response = JobResponse.(JobFailed (Printexc.to_string ex)) in
              JobResponse.send w response;
              run r w)
      | `Ok (JobRequest.ReduceRequest (key, lst)) -> 
          let work = Job.reduce (key, lst) in
          try_with (fun () -> work) 
          >>= (function
          | Core.Std.Ok determined_work -> 
              let response  = JobResponse.(ReduceResult determined_work) in
              JobResponse.send w response;
              run r w
          | Core.Std.Error ex -> 
              let response = JobResponse.(JobFailed (Printexc.to_string ex)) in
              JobResponse.send w response;
              run r w)) 

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


