open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t

let create () : 'a t =
  Pipe.create ()

let push (q: 'a t) (x: 'a) : unit =
  don't_wait_for (Pipe.write (snd q) x)

let pop  (q: 'a t): 'a Deferred.t = 
  (Pipe.read (fst q)) >>= 
  (fun queue -> match queue with
                | `Eof -> failwith "Pipe is closed."
                | `Ok value-> return value)
