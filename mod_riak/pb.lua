-- Prosody library to talk to riak via protobufs
-- Uses Neopallium's protobuf library (https://github.com/Neopallium/lua-pb/)

module:set_global()

local net_server = require "net.server"
local new_fifo = require "fifo"
local uuid_gen = require "util.uuid".generate
local log = require "util.logger".init "riak"
local bit = require "bit"
local function to_uint32_be ( x )
	local o1 = bit.band ( bit.rshift ( x , 24 ) , 0xFF )
	local o2 = bit.band ( bit.rshift ( x , 16 ) , 0xFF )
	local o3 = bit.band ( bit.rshift ( x , 8 ) , 0xFF )
	local o4 = bit.band ( x , 0xFF )
	return string.char ( o1 , o2 , o3 , o4 )
end
local function read_uint32_be ( str , init )
	init = init or 1
	local o1 , o2 , o3 , o4 = str:byte ( init , init + 3 )
	return bit.bor (
		bit.lshift ( o1 , 24 ) ,
		bit.lshift ( o2 , 16 ) ,
		bit.lshift ( o3 , 8 ) ,
		o4 )
end

prosody.unlock_globals()
local pb = require "pb"
-- Set paths to location of .proto files
pb.path = module.path:match(".*/") .. "riak_pb/src/?.proto;"
-- Hack as import is not supported yet
local riak = { }
for _ , name in ipairs { "riak"; "riak_kv"; "riak_dt"; "riak_search"; "riak_yokozuna"; } do
	local lib = pb.require ( name )
	for k , v in pairs ( lib ) do
		riak [ k ] = v
	end
end
prosody.lock_globals()

local msg_codes = {
	[0]  = "RpbErrorResp" ;
	[1]  = "RpbPingReq" ;
	[2]  = "RpbPingResp" ;
	[3]  = "RpbGetClientIdReq" ;
	[4]  = "RpbGetClientIdResp" ;
	[5]  = "RpbSetClientIdReq" ;
	[6]  = "RpbSetClientIdResp" ;
	[7]  = "RpbGetServerInfoReq" ;
	[8]  = "RpbGetServerInfoResp" ;
	[9]  = "RpbGetReq" ;
	[10] = "RpbGetResp" ;
	[11] = "RpbPutReq" ;
	[12] = "RpbPutResp" ;
	[13] = "RpbDelReq" ;
	[14] = "RpbDelResp" ;
	[15] = "RpbListBucketsReq" ;
	[16] = "RpbListBucketsResp" ;
	[17] = "RpbListKeysReq" ;
	[18] = "RpbListKeysResp" ;
	[19] = "RpbGetBucketReq" ;
	[20] = "RpbGetBucketResp" ;
	[21] = "RpbSetBucketReq" ;
	[22] = "RpbSetBucketResp" ;
	[23] = "RpbMapRedReq" ;
	[24] = "RpbMapRedResp" ;
	[25] = "RpbIndexReq" ;
	[26] = "RpbIndexResp" ;
	[27] = "RpbSearchQueryReq" ;
	[28] = "RpbSearchQueryResp" ;
	[29] = "RpbResetBucketReq" ;
	[30] = "RpbSetBucketTypeReq" ;
	[31] = "RpbGetBucketTypeReq" ;
	[32] = "RpbSetBucketTypeReq" ;
	[33] = "RpbCounterUpdateResp" ;
	[40] = "RpbCSBucketReq" ;
	[41] = "RpbCSBucketResp" ;
	[50] = "RpbCounterUpdateReq" ;
	[51] = "RpbCounterUpdateResp" ;
	[52] = "RpbCounterGetReq" ;
	[53] = "RpbCounterGetResp" ;
	[54] = "RpbYokozunaIndexGetReq" ;
	[55] = "RpbYokozunaIndexGetResp" ;
	[56] = "RpbYokozunaIndexPutReq" ;
	[57] = "RpbYokozunaIndexDeleteReq" ;
	[58] = "RpbYokozunaSchemaGetReq" ;
	[59] = "RpbYokozunaSchemaGetResp" ;
	[60] = "RpbYokozunaSchemaPutReq" ;
	[80] = "DtFetchReq" ;
	[81] = "DtFetchResp" ;
	[82] = "DtUpdateReq" ;
	[83] = "DtUpdateResp" ;
}
local parsers = { }
local constructors = { }
for code , name in pairs ( msg_codes ) do
	local msg_type = riak [ name ]
	if msg_type then
		parsers [ code ] = msg_type ( )
		constructors [ name ] = function ( ... )
			local bin = assert ( msg_type ( ... ):Serialize ( ) )
			return to_uint32_be ( 1 + #bin ) .. string.char ( code ) .. bin
		end
	else
		local const_str = "\0\0\0\1" .. string.char ( code )
		constructors [ name ] = function ( )
			return const_str
		end
	end
end
local wait_for_done = { }
for code , name in pairs {
	[16] = "RpbListBucketsResp" ;
	[18] = "RpbListKeysResp" ;
	[24] = "RpbMapRedResp" ;
	[26] = "RpbIndexResp" ;
	[41] = "RpbCSBucketResp" ;
} do
	wait_for_done [ code ] = true
	wait_for_done [ name ] = true
end

local conn_datas = { }
local listener = { }
function listener.onconnect ( conn )
	local conn_data = conn_datas [ conn ]
	conn_data.buffer = ""
	log ( "debug" , "connected, sending client id %s" , conn_data.client_id )
	conn:write ( constructors [ "RpbSetClientIdReq" ] {
		client_id = conn_data.client_id ;
	} )
	conn_data.cb = function ( ok , err )
	end
end
function listener.onincoming ( conn , data )
	local conn_data = conn_datas [ conn ]
	local buffer = conn_data.buffer .. data
	local offset = 0
	while #buffer - offset >= 5 do
		local length = read_uint32_be ( buffer , offset + 1 )
		if #buffer - offset < 4 + length then break end
		local msg_code = buffer:byte ( offset + 5 )
		local parser = parsers [ msg_code ]
		local res
		if parser then
			local data = buffer:sub ( offset + 6 , offset + 4 + length )
			offset = offset + 4 + length
			local err
			res , err = parser:Parse ( data )
			if res == nil then
				log ( "warn" , "failed to parse protobuf %d response: %s" , msg_code , err )
				conn:close ( "invalid response object" )
				return
			end
		elseif msg_codes [ msg_code ] and length == 1 then -- Its a code without contents (e.g. ping)
			offset = offset + 5
			res = true
		else -- Unexpected.... close the connection
			log ( "warn" , "unexpected riak data; dropping connection" )
			conn:close ( "invalid framing data" )
			return
		end
		local cb = conn_data.cb
		conn_data.cb = nil
		local ok , err
		if msg_code == 0 then -- Was an error
			log ( "debug" , "Riak error response: calling callback with nil, %s" , res.errmsg )
			ok , err = pcall ( cb , nil , res.errmsg )
		else
			log ( "debug" , "Calling callback after receiving msg_code %d with %s" , msg_code , tostring(res) )
			ok , err = pcall ( cb , res )
		end
		if not ok then
			log ( "error" , "Traceback: %s" , err )
		end
		if wait_for_done [ msg_code ] and not res.done then
			conn_data.cb = cb
		else
			conn_data.ob:process_next ( conn )
		end
	end
	conn_data.buffer = buffer:sub ( offset + 1 )
end
function listener.ondisconnect ( conn , reason )
	local conn_data = conn_datas [ conn ]
	conn_datas [ conn ] = nil
	conn_data.ob.pool [ conn ] = nil
	local server = conn_data.server
	log ( "debug" , "Riak connection to %s:%d lost: %s" , server.host , server.port , reason )
	server.connected = server.connected - 1
	local cb = conn_data.cb
	if cb then
		conn_data.cb = nil
		log ( "debug" , "connection has inflight request, callback with nil, %s" , reason )
		local ok , err = pcall ( cb , nil , reason )
		if not ok then
			log ( "error" , "Traceback: %s" , err )
		end
	end
end

local methods = { }
local mt = {
	__index = methods ;
}
function new ( servers , client_id )
	return setmetatable ( {
		servers = servers ;
		client_id = client_id or uuid_gen ( ) ;
		pool = { } ; -- Available connections
		queue = new_fifo ( ) ;
	} , mt )
end

function methods:connect ( server )
	local conn , err = net_server.addclient ( server.host , server.port , listener , "*a" , server.sslctx )
	if conn == nil then
		return nil , err
	end
	conn_datas [ conn ] = {
		server = server ;
		client_id = self.client_id ;
		ob = self ;
		buffer = nil ;
		cb = nil ;
	}
	server.connected = ( server.connected or 0 ) + 1 -- Actually only pending...
	return conn
end

function methods:process_next ( conn )
	if self.queue:length ( ) > 0 then
		local conn_data = assert ( conn_datas [ conn ] , "Unknown connection" )
		local req = self.queue:pop ( )
		conn_data.cb = req.cb
		conn:write ( req.data )
	else -- Place into available pool
		self.pool [ conn ] = true
	end
end

function methods:rpc ( name , args , cb )
	local bin = assert ( constructors [ name ] , "Unknown function" ) ( args )
	local conn = next ( self.pool )
	if conn then
		self.pool [ conn ] = nil
		conn_datas [ conn ].cb = cb
		conn:write ( bin )
	else
		local connected = 0
		-- Check if a server isn't at max connections yet
		for _ , server in ipairs ( self.servers ) do
			if ( server.connected or 0 ) < ( server.max_connections or 1 ) then
				self:connect ( server )
			end
			connected = connected + ( server.connected or 0 )
		end

		if connected == 0 then
			cb ( nil , "Unable to connect to a server" )
		else
			self.queue:push ( { name = name ; data = bin , cb = cb } )
		end
	end
end
mt.__call = methods.rpc


