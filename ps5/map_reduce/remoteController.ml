open Async.Std
open AQueue
open Protocol

let addresses = ref []

let init (addrs : (string * int) list) =
	(*turn addrs into 'a where_to_connect so that they'r ready to connect*)
  addresses := List.map (fun (str, port) -> Tcp.to_host_and_port str port) addrs;
  ()

exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

module Make (Job : MapReduce.Job) = struct
  module Response = WorkerResponse(Job)
  module Request = WorkerRequest(Job)
  (* Remember that map_reduce should be parallelized. Note that [Deferred.List]
     functions are run sequentially by default. To achieve parallelism, you may
     find the data structures and functions you implemented in the warmup
     useful. Or, you can pass [~how:`Parallel] as an argument to the
     [Deferred.List] functions.*)
  let map_reduce inputs = 
    let worker_aQueue = create () in
    don't_wait_for(
    (*takes connects workers*)
    Deferred.List.map ~how:`Parallel (!addresses) ~f:Tcp.connect >>= fun connected_workers ->
    (*enqueues connected workers into aQueue*)
    Deferred.List.iter ~how:`Parallel connected_workers ~f:(fun x -> return (push worker_aQueue x)));
    (*sending map requests*)
    Deferred.List.iter ~how:`Parallel inputs ~f:(fun input ->
     return ( (pop worker_aQueue) >>= fun (s, r, w) -> 
    	Request.send w (Request.MapRequest input); push worker_aQueue (s, r, w));
    (*reading map results.... this now only deals with the Queuesel r *)
    Deferred.List.iter ~how:`Parallel inputs ~f:(fun input ->
     return ( (pop worker_aQueue) >>= fun (s, r, w) -> 
    	Request.send w (Request.MapRequest input); push worker_aQueue (s, r, w));


    failwith "uh"


    
end

