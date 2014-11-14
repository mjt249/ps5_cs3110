open Async.Std

(** An asynchronous queue. 'a s can be pushed onto the queue; popping blocks
    until the queue is non-empty and then returns an element. *)
type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t


(** Create a new queue *)
let create () : 'a t =
  Pipe.create ()

(** Add an element to the queue. *)
let push (q: 'a t) (x: 'a) : unit =
  don't_wait_for (Pipe.write (snd q) x)

(** Wait until an element becomes available, and then return it.
    If pipe is closed, raise an exception. *)
let pop  (q: 'a t): 'a Deferred.t = 
  (Pipe.read (fst q)) >>= 
  (fun queue -> match queue with
                | `Eof -> failwith "Pipe is closed."
                | `Ok value-> return value)
