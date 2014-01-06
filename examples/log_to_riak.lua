-- Log messages to riak

local jid_bare = require "util.jid".bare
local riak = module:depends "riak/pb".new ( assert ( module:get_option ( "my_riak_servers" ) ) )
local stanza_bucket_type = module:get_option_string ( "my_riak_type" , "message" )

local function log_message ( bucket , stanza )
	module:log ( "debug" , "Sending stanza to riak %s/%s" , stanza_bucket_type , bucket )
	riak:rpc ( "RpbPutReq" , {
			type = stanza_bucket_type ;
			bucket = bucket ;
			content = {
				value = tostring ( stanza ) ;
				content_type = "application/xml" ;
			} ;
		} , function ( res , err )
			if res == nil then
				module:log ( "warn" , "Unable to send stanza to riak: %s" , err )
			else
				module:log ( "debug" , "Saved stanza to riak under %s/%s/%s" , stanza_bucket_type , bucket , res.key )
			end
		end )
end

module:hook ( "message/bare" , function ( event )
	log_message ( event.stanza.from , event.stanza )
end )
module:hook ( "message/full" , function ( event )
	log_message ( jid_bare ( event.stanza.from ) , event.stanza )
end )
