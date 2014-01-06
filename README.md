prosody-riak
============

Riak module for prosody; currently only hits the protobuf interface, and the API is very low level.

The library is non-blocking, and performs connection pooling under the hood for you.


# Dependencies

  - lua-pb


# Usage

`handle = pb.new ( servers [, client_id] )`

Servers is an array of dictionaries with fields:

  - `host` (required string)
  - `port` (required number)
  - `sslctx` (optional table)
  - `max_connections` (optional number)

## Example

    myriak = module:depends "riak/pb".new {
		{ host = "127.0.0.1" , port = 8087 , max_connections = 10 } ;
	}
