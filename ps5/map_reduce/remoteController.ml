open Async.Std
open AQueue
open Protocol

let worker_ides = ref []

  
  (*Takes string, int tuples of ports and makes a list of 
  'a where_to_connect 's so that they're ready to for connecting. *)
let init (addrs : (string * int) list) =
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
    let num_mappers = ref 0 in

    let reduce_workers = create () in
    let num_reducers = ref 0 in

    let map_result = ref (return []) in
    let combined_result = ref (return []) in
    let reduced_result = ref [] in

    let input_queue = create () in
    let num_input = ref (List.length(inputs)) in

    let key_inter_queue = create () in
    let num_keys = ref 0 in
    
    (*Wrapper function for Tcp.connect to catch connection errrors.
      If successfully connected, the worker is enqueued to the idle worker
      queue. If there is an error, nothing is done.*)
    let connect_wrapper worker_id =
      try_with (fun () -> (Tcp.connect worker_id)) 
        >>| (function
              | Core.Std.Ok (socket, reader, writer) -> 
                  (*send Job name string to the worker to get run going.*)
                  Writer.write_line writer Job.name;
                  push idle_workers (socket, reader, writer);
                  num_idle := !num_idle +1 
              | Core.Std.Error _ -> () ) in
    
    (*Connects workers and enqueues successfully connected workers to the 
      idle worker queue.*)
    let connect_workers : unit -> unit Deferred.t = fun () ->
      Deferred.List.iter ~how:`Parallel (!worker_ides) ~f: connect_wrapper in

    (*Enqueues all given inputs.*)
    let enqueue_inputs : unit -> unit Deferred.t = fun () ->
      Deferred.List.iter ~how:`Parallel inputs 
        ~f: fun input -> return(push input_queue input ) in

    (*Dequeues an idle worker from idle queue, sends a map request of input and
      enqueues the worker to the map worker queue.*)
    let map_helper input = function
      (socket, reader, writer) -> 
          Request.send writer (Request.MapRequest input); 
          num_mappers := !num_mappers +1; 
          num_idle := !num_idle -1;
          return (push map_workers (input, (socket, reader, writer))) in
    
    (*The map portion of map reduce. Workers map all given inputs and the results
      are accumulated to be used in the reduce phase.*)
    let rec map_phase: unit -> unit Deferred.t = fun () ->

      (*There are no idle workers and no workers mapping, 
        so no workers available at all. Raise InfrastructureFailure.*)
      if ((!num_idle = 0) && (!num_mappers = 0)) then
        raise InfrastructureFailure

      (*All inputs have been mapped and all workers are done mapping.
        flatten and combine the accumulated map_result so that they can
        be used in the reduce phase.*)
      else if ((!num_input = 0) && (!num_mappers = 0)) then
        !map_result >>= fun res_list ->
          combined_result := ((!map_result) 
                          >>| List.flatten
                          >>| Combine.combine);
          !combined_result >>= fun key_inter_pairs -> 
          num_keys := List.length key_inter_pairs;
          Deferred.List.iter ~how:`Parallel 
            key_inter_pairs ~f: fun el -> return (push key_inter_queue el)

      (*Unmapped inputs left and workers available. Send a map request.*)
      else if ((!num_input > 0) && (!num_idle > 0)) then 
        pop input_queue >>= fun input ->
          num_input := !num_input -1;
          don't_wait_for (pop idle_workers >>= map_helper input);
          map_phase () 

      (*No idle workers are available, or controller only has to read responses.
        Therefore, read from a worker that recieved a map request.*)
      else if (!num_mappers > 0) then
        pop map_workers >>= fun (input, (socket, reader, writer)) -> 
          num_mappers := !num_mappers - 1;
          Response.receive reader >>= function 
            |(`Ok (Response.MapResult key_inter_pair_list)) -> 
              !map_result >>= fun mappings -> 
                map_result := return (key_inter_pair_list::mappings);
                push idle_workers (socket, reader, writer); 
                num_idle := !num_idle +1;
                map_phase ()
            |(`Ok Response.ReduceResult _ )-> 
                failwith "Worker performing reduce phase during map phase."
            | _ -> 
                don't_wait_for (Writer.close writer);
                push input_queue input;
                num_input := !num_input +1;
                map_phase()
       else
         failwith "Map phase invariants broken" in

    (*Dequeues an idle worker from idle queue, sends a reduce request of 
      key, inter list and enqueues the worker to the reduce worker queue.*)
    let reduce_helper key lst = 
      function (socket, reader, writer) ->
    	  Request.send writer (Request.ReduceRequest (key, lst));
        num_reducers := !num_reducers +1;
        num_idle := !num_idle -1;
    	  return (push reduce_workers (key, lst, (socket, reader, writer))) in

    (*If no workers are available and key inter pairs still need to be reduced,
      raises InfrastructureFailure. 
      If all key inter pairs have sent to be reduced, and no reduce requests 
      remain, close all connections of the idle workers and 
      return the reduced result.*)
    let rec reduce_phase : unit -> (Job.key * Job.output) list Deferred.t = fun () ->

      (*All key inter pairs have been reduced but workers are still connected.
        Close all connections.*)
      if ((!num_keys = 0) && (!num_reducers = 0) && (!num_idle > 0)) then 
        pop idle_workers >>= fun (socket, reader, writer) ->
          num_idle := !num_idle -1;
          don't_wait_for(Writer.close writer);
          reduce_phase ()

      (*All key inter pairs have been reduced and all workers are done reducing.
        Return the accumulated result.*)
      else if ((!num_keys = 0) && (!num_reducers = 0) && (!num_idle = 0)) then 
        return(!reduced_result)

      (*No idle workers and no workers reducing, so no workers available at all.
        raise InfrastructureFailure.*)
      else if ((!num_idle = 0) && (!num_reducers = 0)) then
        raise InfrastructureFailure

      (*Still need to send reduce requests and there is a worker avaialbe.
        send a reduce request.*)
      else if ((!num_keys > 0) && (!num_idle > 0)) then 
        pop key_inter_queue >>= fun (key, lst) ->
          num_keys := !num_keys -1;
          don't_wait_for (pop idle_workers >>= reduce_helper key lst);
          reduce_phase ()

      (*No idle workers are available, or controller only has to read responses.
        Read from a worker that recieved a reduce request.*)
      else if (!num_reducers > 0) then
        pop reduce_workers >>= fun (key, lst, (socket, reader, writer)) -> 
          num_reducers := !num_reducers -1;
          Response.receive reader >>= function
            |(`Ok (Response.ReduceResult output)) -> 
                reduced_result := ((key, output)::(!reduced_result));
                push idle_workers (socket, reader, writer); 
                num_idle := !num_idle +1;
                reduce_phase ()
            |(`Ok Response.MapResult _ )-> 
                failwith "Workers performing mapping during reduce phase"
            | _ -> 
                don't_wait_for (Writer.close writer);
                push key_inter_queue (key, lst);
                num_keys := !num_keys +1;
                reduce_phase()
       else
         failwith "Reduce phase invariants broken" in

    connect_workers () >>=
    enqueue_inputs >>=
    map_phase >>=
    reduce_phase
end
