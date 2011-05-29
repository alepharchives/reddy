-module(reddy_sorted_sets).

-include("reddy_ops.hrl").

-export([zadd/4,
	 zcard/2,
	 zcount/4,
	 zincrby/4,
%	 zinterstore/3,
	 zrange/5,
	 zrange/4,
%	 zrangebyscore/4,
	 zrank/3,
	 zrem/3,
%	 zremrangebyrank/4,
	 zremrangebyscore/4,
%	 zrevrange/4,
%	 zrevrangebyscore/4,1
	 zrevrank/3,
	 zscore/3
%	 zunionstore/3
	]).

zadd(Conn, Key, Score, Member) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZADD, [Key, Score, Member]);
zadd(Pool, Key, Score, Member) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zadd, [Key, Score, Member]).

zcard(Conn, Key) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZCARD, [Key]);
zcard(Pool, Key) ->
    ?WITH_POOL(Pool, zrange, [Key]).

zcount(Conn, Key, Min, Max) when is_pid(Conn) ->
    not_implemented;
zcount(Pool, Key, Min, Max) ->
    not_implemented.

zincrby(Conn, Key, Increment, Member) when is_pid(Conn) ->
    not_implemented;
zincrby(Pool, Key, Increment, Member) ->
    not_implemented.

zrange(Conn, Key, Start, Stop, with_scores) when is_pid(Conn) ->
    create_score_reply(reddy_conn:sync(Conn, ?ZRANGE, [Key, Start, Stop, "WITHSCORES"]));
zrange(Pool, Key, Start, Stop, with_scores) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zrange, [Key, Start, Stop, "WITHSCORES"]).

zrange(Conn, Key, Start, Stop) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZRANGE, [Key, Start, Stop]);
zrange(Pool, Key, Start, Stop) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zrange, [Key, Start, Stop]).

zrank(Conn, Key, Member) when is_pid(Conn) ->
    not_implemented;
zrank(Pool, Key, Member) ->
    not_implemented.

zrem(Conn, Key, Member) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZREM, [Key, Member]);
zrem(Pool, Key, Member) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zrem, [Key, Member]).

zremrangebyscore(Conn, Key, Min, Max) when is_pid(Conn) ->
    not_implemented;
zremrangebyscore(Pool, Key, Min, Max) ->
    not_implemented.

zrevrank(Conn, Key, Member) when is_pid(Conn) ->
    not_implemented;
zrevrank(Pool, Key, Member) ->
    not_implemented.

zscore(Conn, Key, Member) when is_pid(Conn) ->
    not_implemented;
zscore(Pool, Key, Member) ->
    not_implemented.

%% Internal
create_score_reply(List) ->
    create_score_reply(List, []).
create_score_reply([Value, Score|Rest], Return) ->
    create_score_reply(Rest, Return++[{Value, binary_to_number(Score)}]);
create_score_reply([], Return) ->
    Return.

binary_to_number(Binary) ->
    List = binary_to_list(Binary),
    try list_to_float(List)
    catch
	error: badarg ->
	    list_to_integer(List)
    end.
