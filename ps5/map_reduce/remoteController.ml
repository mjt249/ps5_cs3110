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
    let map_response = ref (return []) in
    let number_of_inputs_mapped = ref 0 in
    let shuffled_maps = ref (return []) in
    let ready_for_reduce = ref false in


    don't_wait_for(
    (*connects workers*)
    Deferred.List.map ~how:`Parallel (!addresses) ~f:Tcp.connect >>= fun connected_workers ->
    (*enqueues connected workers into aQueue*)
    Deferred.List.iter ~how:`Parallel connected_workers ~f:(fun x -> return (push idle_worker_aQueue x)));
    (*sending map requests and enqueueing map workers*)
    Deferred.List.iter ~how:`Parallel inputs ~f:(fun input ->
     return ( (pop worker_aQueue) >>= fun (s, r, w) -> 
    	Request.send w (Request.MapRequest input); push mapping_worker_aQueue (s, r, w));
    (*read map results from busy workers*)
    

    let rec read_mapping =
      if (number_of_inputs_mapped = List.length(inputs)) then
        map_response >>= fun res_list ->
        (*shuffule map_response!!*)
        shuffuled_maps := ((!map_response) 
                          >>| Combine.combine);
        ready_for_reduce := true;
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
              read_mapping
          (*will be JobFailed because this Queue only had map requests.
            need a way to find out which input JobFailes came from before I can send the input to another worker.*)
            | _ -> read_mapping in

    let rec reduce_mappings 

    don't_wait_for (read_mapping);






    
end

