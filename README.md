prosody-riak
============

Riak module for prosody; currently only hits the protobuf interface, and the API is very low level.

The library is non-blocking, and performs connection pooling under the hood for you.


## Dependencies

  - [lua-pb](https://github.com/Neopallium/lua-pb/)


## Usage

Use `module:depends` to load this library

### handle = pb.new ( servers [, client_id] )`

`servers` is an array of dictionaries with fields:

  - `host` (required string)
  - `port` (required number)
  - `sslctx` (optional table)
  - `max_connections` (optional number)


### `handle:rpc ( message_type , data , callback )`

`message_type` is the riak command you want to issue; a list can be found in the [riak documentation](http://docs.basho.com/riak/latest/dev/references/protocol-buffers/)  
`data` can be `nil` for commands that have no parameters; otherwise it is a   
`callback` will be called with the response as a single argument or `(nil,err)` in case of error


## Example

    myriak = module:depends "riak/pb".new {
		{ host = "127.0.0.1" , port = 8087 , max_connections = 10 } ;
	}
	myriak:rpc ( "RpbPingReq" , nil , function ( res , err )
		if res == nil then
			print ( "Failure!" , err )
			return
		end
		print ( "PONG" )
	end )

