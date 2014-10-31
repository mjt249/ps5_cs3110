open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t


let create () : 'a t =
  Pipe.create ()

let push (q: 'a t) (x: 'a) : unit =
  ignore(Pipe.write (snd q) x)

let pop  (q: 'a t): 'a Deferred.t = 
  (Pipe.read (fst q)) >>= (fun read_q ->
  match read_q with
  | `Eof -> failwith "isEmpty"
  | `Ok value-> return value)