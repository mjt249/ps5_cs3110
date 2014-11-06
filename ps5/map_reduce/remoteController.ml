open Async.Std
open AQueue
open Protocol

let worker_ides = ref []

let init (addrs : (string * int) list) =
	(*turn addrs into 'a where_to_connect so that they'r ready to connect*)
  worker_ides := List.map (fun (str, port) -> Tcp.to_host_and_port str port) addrs;
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

    let idle_workers = create () in
    let map_workers = create () in
    let reduce_workers = create () in

    let map_result = ref (return []) in
    let number_of_inputs_mapped = ref 0 in

    let combined_result = ref (return []) in
    
    let number_of_keys = ref 0 in
    let number_of_pairs_reduced = ref 0 in
    
    let reduced_result = ref (return []) in

    let workers_down = ref 0 in

    let connect_wrapper worker_id =
      try_with (fun () -> (Tcp.connect worker_id)) 
        >>| (function
        | Core.Std.Ok connected -> push idle_workers (worker_id, connected)
        | Core.Std.Error _ -> workers_down := !workers_down + 1;
            () ) in
    
    let connect_workers : unit -> unit Deferred.t = fun () ->
    (*connects workers and enqueues them*)
      Deferred.List.iter ~how:`Parallel (!worker_ides) ~f: connect_wrapper in

    let map_helper input = function
      (worker_id, (s, r, w)) -> 
          Request.send w (Request.MapRequest input); 
          return (push map_workers (input, (worker_id, (s, r, w)))) in
(*       (worker_id, connected) -> 
        connected >>= fun (s, r, w) ->
          Request.send w (Request.MapRequest input); 
          return (push map_workers (input, (worker_id, (s, r, w))))
      | _ -> failwith "map_helper is confused... and I'm confused." in *)

    let send_map_requests : unit -> unit Deferred.t = fun () ->
    (*sending map requests and enqueueing map workers*)
    Deferred.List.iter ~how:`Parallel inputs ~f:(fun input ->
    	(*pop idle_workers has type (worker_id, ((s,r,w) deferred.t)) deferred.t *)
     (pop idle_workers) >>= (map_helper input) ) in
      		
    
    let rec read_map_response : unit -> unit Deferred.t = fun () ->
      if ((!workers_down) = List.length(!worker_ides)) then
        raise InfrastructureFailure
      else if ((!number_of_inputs_mapped) = List.length(inputs)) then
        !map_result >>= fun res_list ->
        (*shuffle map_result!!*)
        combined_result := ((!map_result) 
        	              >>| List.flatten
                          >>| Combine.combine);
        !combined_result >>= fun num ->
        number_of_keys := List.length(num);
        return ()
      else
        pop map_workers >>= fun (input, (worker_id, (s, r, w))) -> 
          (Response.receive r)>>= function 
            |(`Ok (Response.MapResult key_inter_pair_list)) -> 
              !map_result >>= fun mappings -> 
              (*adding response pairs to list*)
              (*I'm flattening the list here already. should I not flatten and do
              >>| List.flatten later in the if clause above?*)
              map_result := return(key_inter_pair_list::mappings);
(*               map_result := Deferred.List.fold ~how:`Parallel mappings key_inter_pair_list ~f:(fun acc el -> return (el::acc)); *)
              (*number of inputs successfully mapped and added*)
              number_of_inputs_mapped := !number_of_inputs_mapped + 1;
              connect_wrapper worker_id >>= fun _ ->
              read_map_response ()
            |(`Ok Response.ReduceResult _ )-> failwith "map queue is confused... it's reducing" 
            | _ -> workers_down := !workers_down +1;
                   ((pop idle_workers) >>= (map_helper input)) >>= fun _ ->
                   read_map_response () in


    let reduce_helper key lst = function
      (worker_id, (s, r, w)) ->
    	  Request.send w (Request.ReduceRequest (key, lst) );
    	  return (push reduce_workers (key, lst, (worker_id, (s, r, w)))) in
(*       (worker_id, connected) ->
        connected >>= fun (s, r, w) -> 
    	  Request.send w (Request.ReduceRequest (key, lst) );
    	  return (push reduce_workers (key, lst, (worker_id, (s, r, w))))
      | _ -> failwith "reduce_helper is confused too :( " in *)

    
    let send_reduce_requests : unit -> unit Deferred.t = fun () ->
      !combined_result >>= fun comb_lst ->
      Deferred.List.iter ~how:`Parallel (comb_lst) ~f:(fun (key, lst) ->
        (pop idle_workers) >>= (reduce_helper key lst)) in

    let rec read_reduce_response : unit -> (Job.key * Job.output) list Deferred.t = fun () ->
      if ((!workers_down) = List.length(!worker_ides)) then
        raise InfrastructureFailure
      else if ((!number_of_pairs_reduced) = (!number_of_keys)) then 
        !reduced_result
      else
        pop reduce_workers >>= fun (key, lst, (worker_id, (s, r, w))) -> 
          Response.receive r >>= function
            |(`Ok (Response.ReduceResult output)) -> 
              !reduced_result >>= fun results -> 
              (*adding response pairs to list*)
              reduced_result := return((key, output)::results);
              (*number of inputs successfully mapped and added*)
              number_of_pairs_reduced := !number_of_pairs_reduced + 1;
              connect_wrapper worker_id >>= fun _ ->
              read_reduce_response ()
          (*will be JobFailed because this Queue only had reduce requests.
            need a way to find out which input JobFailes came from before I can send the input to another worker.*)
            |(`Ok Response.MapResult _ )-> failwith "reduce queue is confused... it's mapping" 
            | _ -> workers_down := !workers_down +1;
                  ((pop idle_workers) >>= (reduce_helper key lst)) >>= fun _ ->
                  read_reduce_response () in

    connect_workers () >>=
    send_map_requests >>=
    read_map_response >>=
    send_reduce_requests >>=
    read_reduce_response 
end

