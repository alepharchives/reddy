-module(reddy_sorted_sets).

-include("reddy_ops.hrl").

-export([zadd/4,
	 zcard/2,
	 zcount/4,
	 zincrby/4,
%	 zinterstore/3,
	 zrange/5, %CHECK
%	 zrangebyscore/4, %CHECK
	 zrank/3,
	 zrem/3,
%	 zremrangebyrank/4,
	 zremrangebyscore/4,
%	 zrevrange/4,
%	 zrevrangebyscore/4,
	 zrevrank/3,
	 zscore/3
%	 zunionstore/3
	]).

zadd(Conn, Key, Score, Member) when is_pid(Conn),
				    is_integer(Score);
				    is_float(Score) ->
    reddy_conn:sync(Conn, ?ZADD, [Key, Score, Member]);
zadd(Pool, Key, Score, Member) when is_atom(Pool),
				    is_integer(Score); 
				    is_float(Score) ->
    ?WITH_POOL(Pool, zadd, [Key, Score, Member]).

zcard(Conn, Key) when is_pid(Conn) ->
    not_implemented;
zcard(Pool, Key) ->
    not_implemented.

zcount(Conn, Key, Min, Max) when is_pid(Conn) ->
    not_implemented;
zcount(Pool, Key, Min, Max) ->
    not_implemented.

zincrby(Conn, Key, Increment, Member) when is_pid(Conn) ->
    not_implemented;
zincrby(Pool, Key, Increment, Member) ->
    not_implemented.

zrange(Conn, Key, Start, Stop, true) when is_pid(Conn) ->
    not_implemented;
zrange(Pool, Key, Start, Stop, false) ->
    not_implemented.

zrank(Conn, Key, Member) when is_pid(Conn) ->
    not_implemented;
zrank(Pool, Key, Member) ->
    not_implemented.

zrem(Conn, Key, Member) when is_pid(Conn) ->
    not_implemented;
zrem(Pool, Key, Member) ->
    not_implemented.

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



