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
    let num_idle = ref 0 in

    let map_workers = create () in
    let num_mapper = ref 0 in

    let reduce_workers = create () in
    let num_reducer = ref 0 in

    let map_result = ref (return []) in

    let combined_result = ref (return []) in
    
    let reduced_result = ref (return []) in

    let input_q = create () in
    let num_input = ref (List.length(inputs)) in

    let key_q = create () in
    let num_keys = ref 0 in

    let connect_wrapper worker_id =
      try_with (fun () -> (Tcp.connect worker_id)) 
        >>| (function
        | Core.Std.Ok connected -> push idle_workers (worker_id, connected);
                                   num_idle := !num_idle +1 
        | Core.Std.Error _ -> () ) in
    
    let connect_workers : unit -> unit Deferred.t = fun () ->
    (*connects workers and enqueues them*)
      Deferred.List.iter ~how:`Parallel (!worker_ides) ~f: connect_wrapper in

    let enqueue_inputs : unit -> unit Deferred.t = fun () ->
      Deferred.List.iter ~how:`Parallel inputs ~f: fun input -> return(push input_q input ) in

    let map_helper input = function
      (worker_id, (s, r, w)) -> 
          Request.send w (Request.MapRequest input); 
          num_mapper := !num_mapper +1;
          return (push map_workers (input, (worker_id, (s, r, w)))) in
    
    let rec map_phase: unit -> unit Deferred.t = fun () ->
      if ((!num_idle = 0) && (!num_mapper = 0)) then
        raise InfrastructureFailure
      else if ((!num_input = 0) && (!num_mapper = 0)) then
        !map_result >>= fun res_list ->
        (*shuffle map_result!!*)
        combined_result := ((!map_result) 
        	              >>| List.flatten
                          >>| Combine.combine);
        !combined_result >>= fun num ->
        num_keys := List.length(num);
        return ()
      else if ((!num_input > 0) && (!num_idle > 0)) then 
        pop input_q >>= fun input ->
        pop idle_workers >>= map_helper input 
        >>= fun _ -> 
        num_idle := !num_idle -1;
        num_input := !num_input -1;
        map_phase ()
      else if ((!num_input = 0) || (!num_idle = 0)) then
        pop map_workers >>= fun (input, (worker_id, (s, r, w))) -> 
          num_mapper := !num_mapper - 1;
          (Response.receive r)>>= function 
            |(`Ok (Response.MapResult key_inter_pair_list)) -> 
              !map_result >>= fun mappings -> 
              map_result := return(key_inter_pair_list::mappings);
              connect_wrapper worker_id >>= fun _ ->
              map_phase ()
            |(`Ok Response.ReduceResult _ )-> failwith "map queue is confused... it's reducing" 
            | _ -> push input_q input;
                   num_input := !num_input +1;
                   map_phase()
       else
         failwith "map gone wrong" in


    let enqueue_keys : unit -> unit Deferred.t = fun () ->
      !combined_result >>= fun lst ->
      Deferred.List.iter ~how:`Parallel lst ~f: fun el -> return(push key_q el) in

    let reduce_helper key lst = function
      (worker_id, (s, r, w)) ->
    	  Request.send w (Request.ReduceRequest (key, lst) );
          num_reducer := !num_reducer +1;
    	  return (push reduce_workers (key, lst, (worker_id, (s, r, w)))) in

    let rec reduce_phase : unit -> (Job.key * Job.output) list Deferred.t = fun () ->
      if ((!num_idle = 0) && (!num_reducer = 0)) then
        raise InfrastructureFailure
      else if ((!num_keys = 0) && (!num_reducer = 0)) then 
        !reduced_result
      else if ((!num_keys > 0) && (!num_idle > 0)) then 
        pop key_q >>= fun (key, lst) ->
        pop idle_workers >>= (reduce_helper key lst)
        >>= fun _ -> 
        num_idle := !num_idle -1;
        num_keys := !num_keys -1;
        reduce_phase ()
      else if ((!num_keys = 0) || (!num_idle = 0)) then
        pop reduce_workers >>= fun (key, lst, (worker_id, (s, r, w))) -> 
          Response.receive r >>= function
            |(`Ok (Response.ReduceResult output)) -> 
              !reduced_result >>= fun results -> 
              (*adding response pairs to list*)
              reduced_result := return((key, output)::results);
              connect_wrapper worker_id >>= fun _ ->
              reduce_phase ()
          (*will be JobFailed because this Queue only had reduce requests.
            need a way to find out which input JobFailes came from before I can send the input to another worker.*)
            |(`Ok Response.MapResult _ )-> failwith "reduce queue is confused... it's mapping" 
            | _ -> push key_q (key, lst);
                   num_keys := !num_keys +1;
                   reduce_phase()
       else
         failwith "reduce gone wrong" in

    connect_workers () >>=
    enqueue_inputs >>=
    map_phase >>=
    enqueue_keys >>=
    reduce_phase
end

