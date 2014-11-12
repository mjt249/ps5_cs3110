open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t

let create () =
  Pipe.create ()

let push q x =
  ignore(Pipe.write (snd q) x )

let pop  q =
  Pipe.read (fst q) >>= fun contents -> match contents with 
  | `Eof -> failwith "Pipe is closed"
  | `Ok x -> return x
