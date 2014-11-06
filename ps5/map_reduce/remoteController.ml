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

  (* Remember that map_reduce should be parallelized. Note that [Deferred.List]
     functions are run sequentially by default. To achieve parallelism, you may
     find the data structures and functions you implemented in the warmup
     useful. Or, you can pass [~how:`Parallel] as an argument to the
     [Deferred.List] functions.*)

  module Response = WorkerResponse(Job)
  module Request = WorkerRequest(Job)
  module Combine = Combiner.Make(Job)

  let map_reduce inputs = 
    let idle_worker_aQueue = create () in
    let mapping_worker_aQueue = create () in
    let reducing_worker_aQueue = create () in
    let map_response = ref (return []) in
    let number_of_inputs_mapped = ref 0 in
    let shuffled_maps = ref (return []) in
    let number_of_keys = ref 0 in
    let number_of_pairs_reduced = ref 0 in
    let reduced_result = ref (return []) in

    let connect_workers : unit -> unit Deferred.t = fun () ->
    (*connects workers*)
      Deferred.List.map ~how:`Parallel (!addresses) ~f:Tcp.connect >>= 
        fun connected_workers ->
    (*enqueues connected workers into aQueue*)
        Deferred.List.iter ~how:`Parallel connected_workers ~f:(fun x -> 
        	return (push idle_worker_aQueue x)) in

    let send_map_requests : unit -> unit Deferred.t = fun () ->
    (*sending map requests and enqueueing map workers*)
    Deferred.List.iter ~how:`Parallel inputs ~f:(fun input ->
     return ( (pop worker_aQueue) >>= fun (s, r, w) -> 
    	Request.send w (Request.MapRequest input); push mapping_worker_aQueue (s, r, w))) in
    (*read map results from mapping workers*)

    let send_reduce_requests : unit -> unit Deferred.t = fun () ->
      Deferred.List.iter ~how:`Parallel (!shuffled_maps) ~f:(fun (key, lst) -> 
      	return ( (pop idle_worker_aQueue) >>= fun (s, r, w) -> 
    	  Request.send w (Request.ReduceRequest pair); push reducing_worker_aQueue (key ,(s, r, w))) in
    

    let rec read_map_response : unit -> unit Deferred.t = fun () ->
      if ((!number_of_inputs_mapped) = List.length(inputs)) then
        map_response >>= fun res_list ->
        (*shuffule map_response!!*)
        shuffled_maps := ((!map_response) 
                          >>| Combine.combine);
        number_of_keys := List.length(!shuffled_maps);
        return ()
      else
        pop mapping_worker_aQueue >>= fun (s, r, w) -> 
          Response.receive r >>= fun res -> function
            (`Ok (MapResult pairs)) -> 
              map_response >>= fun mappings -> 
              (*adding response pairs to list*)
              (*I'm flattening the list here already. should I not flatten and do
              >>| List.flatten later in the if clause above?*)
              map_response := Deferred.List.fold ~how:`Parallel mappings pairs ~f:(fun acc el -> return (el::acc));
              (*number of inputs successfully mapped and added*)
              number_of_inputs_mapped := number_of_inputs_mapped + 1;
              push idle_worker_aQueue (s, r, w);
              read_map_response
          (*will be JobFailed because this Queue only had map requests.
            need a way to find out which input JobFailes came from before I can send the input to another worker.*)
            | _ -> read_map_response in

    let rec read_reduce_response: unit -> (MapReduce.Job.key * MapReduce.Job.output) list Deferred.t = fun () ->
      if ((!number_of_pairs_reduced) = (!number_of_keys)) then 
         return (!results)
      else
        pop reducing_worker_aQueue >>= fun (key, (s, r, w)) -> 
          Response.receive r >>= fun res -> function
            (`Ok (ReduceResult output)) -> 
              reduced_result >>= fun results -> 
              (*adding response pairs to list*)
              results := ((key, output)::results);
              (*number of inputs successfully mapped and added*)
              number_of_pairs_reduced := number_of_pairs_reduced + 1;
              push idle_worker_aQueue (s, r, w);
              read_reduce_response
          (*will be JobFailed because this Queue only had reduce requests.
            need a way to find out which input JobFailes came from before I can send the input to another worker.*)
            | _ -> read_reduce_response in

    connect_workers () >>=
    send_map_requests >>=
    read_map_response >>=
    send_reduce_requests >>=
    read_reduce_requests 
end

