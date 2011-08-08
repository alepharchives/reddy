-module(reddy_pubsub).

-include("reddy_ops.hrl").

-export([publish/3]).

publish(Conn, Channel, Message) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?PUBLISH, [Channel, Message]);
publish(Pool, Channel, Message) ->
    ?WITH_POOL(Pool, publish, [Channel, Message]).
