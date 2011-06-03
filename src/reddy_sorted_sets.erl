-module(reddy_sorted_sets).

-include("reddy_ops.hrl").

-export([zadd/4,
	 zcard/2,
	 zcount/4,
	 zincrby/4,
%	 zinterstore/3,
	 zrange/5,
	 zrange/4,
	 zrangebyscore/6,
	 zrangebyscore/5,
	 zrangebyscore/4,
	 zrank/3,
	 zrem/3,
	 zremrangebyrank/4,
%	 zremrangebyscore/4,
	 zrevrange/5,
	 zrevrange/4,
%	 zrevrangebyscore/4,1
%	 zrevrank/3,
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

zcount(Conn, Key, Min, Max) when is_pid(Conn),
				 is_integer(Min);
				 is_integer(Max);
				 is_list(Min);
				 is_list(Max) ->
    reddy_conn:sync(Conn, ?ZCOUNT, [Key, Min, Max]);
zcount(Pool, Key, Min, Max) when is_atom(Pool),
				 is_integer(Min);
				 is_integer(Max);
				 is_list(Min);
				 is_list(Max) ->
    ?WITH_POOL(Pool, zcount, [Key, Min, Max]).

zincrby(Conn, Key, Increment, Member) when is_pid(Conn),
					    is_integer(Increment); 
					    is_float(Increment) ->
    reddy_types:binary_to_number(reddy_conn:sync(Conn, ?ZINCRBY, [Key, Increment, Member]));
zincrby(Pool, Key, Increment, Member) when is_atom(Pool),
					   is_integer(Increment);
					   is_float(Increment) ->
    reddy_types:binary_to_number(?WITH_POOL(Pool, ?ZINCRBY, [Key, Increment, Member])).

zrange(Conn, Key, Start, Stop, WithScores) when is_pid(Conn),
						WithScores =:= "WITHSCORES"->
    reddy_types:create_score_reply(reddy_conn:sync(Conn, ?ZRANGE, [Key, Start, Stop, WithScores]));
zrange(Pool, Key, Start, Stop, WithScores) when is_atom(Pool),
						WithScores =:= "WITHSCORES"->
    reddy_types:create_score_reply(?WITH_POOL(Pool, zrange, [Key, Start, Stop, WithScores])).

zrange(Conn, Key, Start, Stop) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZRANGE, [Key, Start, Stop]);
zrange(Pool, Key, Start, Stop) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zrange, [Key, Start, Stop]).

zrank(Conn, Key, Member) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZRANK, [Key, Member]);
zrank(Pool, Key, Member) ->
    ?WITH_POOL(Pool, ?ZRANK, [Key, Member]).

zrem(Conn, Key, Member) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZREM, [Key, Member]);
zrem(Pool, Key, Member) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zrem, [Key, Member]).

zremrangebyrank(Conn, Key, Min, Max) when is_pid(Conn),
					   is_integer(Min),
					   is_integer(Max) ->
    reddy_conn:sync(Conn, ?ZREMRANGEBYRANK, [Key, Min, Max]);
zremrangebyrank(Pool, Key, Min, Max) when is_atom(Pool),
					   is_integer(Min),
					   is_integer(Max) ->
    ?WITH_POOL(Pool, zremrangebyrank, [Key, Min, Max]).

zrevrange(Conn, Key, Start, Stop, WithScores) when is_pid(Conn),
						WithScores =:= "WITHSCORES"->
    reddy_types:create_score_reply(reddy_conn:sync(Conn, ?ZREVRANGE, [Key, Start, Stop, WithScores]));
zrevrange(Pool, Key, Start, Stop, WithScores) when is_atom(Pool),
						WithScores =:= "WITHSCORES"->
    reddy_types:create_score_reply(?WITH_POOL(Pool, zrevrange, [Key, Start, Stop, WithScores])).

zrevrange(Conn, Key, Start, Stop) when is_pid(Conn) ->
    reddy_conn:sync(Conn, ?ZREVRANGE, [Key, Start, Stop]);
zrevrange(Pool, Key, Start, Stop) when is_atom(Pool) ->
    ?WITH_POOL(Pool, zrevrange, [Key, Start, Stop]).

zscore(Conn, Key, Member) when is_pid(Conn) ->
    reddy_types:binary_to_number(reddy_conn:sync(Conn, ?ZSCORE, [Key, Member]));
zscore(Pool, Key, Member) ->
    reddy_types:binary_to_number(?WITH_POOL(Pool, zscore, [Key, Member])).

zrangebyscore(Conn, Key, Min, Max, WithScores, Limit) when is_pid(Conn),
							   is_integer(Min); is_float(Min); is_list(Min),
							   is_integer(Max); is_float(Max); is_list(Max),
							   WithScores =:= "WITHSCORES",
							   is_integer(Limit) ->
    reddy_types:create_score_reply(reddy_conn:sync(Conn, ?ZRANGEBYSCORE, [Key, Min, Max, WithScores, "LIMIT", Limit]));
zrangebyscore(Pool, Key, Min, Max, WithScores, Limit) when is_atom(Pool),
							   is_integer(Min); is_float(Min); is_list(Min),
							   is_integer(Max); is_float(Max); is_list(Max),
							   WithScores =:= "WITHSCORES",
							   is_integer(Limit) ->
    reddy_types:create_score_reply(?WITH_POOL(Pool, ?ZRANGEBYSCORE, [Key, Min, Max, WithScores, "LIMIT", Limit])).

zrangebyscore(Conn, Key, Min, Max, WithScores) when is_pid(Conn),
						    is_integer(Min); is_float(Min); is_list(Min),
						    is_integer(Max); is_float(Max); is_list(Max),
						    WithScores =:= "WITHSCORES" ->
    reddy_types:create_score_reply(reddy_conn:sync(Conn, ?ZRANGEBYSCORE, [Key, Min, Max, WithScores]));
zrangebyscore(Pool, Key, Min, Max, WithScores) when is_atom(Pool),
						    is_integer(Min); is_float(Min); is_list(Min),
						    is_integer(Max); is_float(Max); is_list(Max),
						    WithScores =:= "WITHSCORES" ->
    reddy_types:create_score_reply(?WITH_POOL(Pool, ?ZRANGEBYSCORE, [Key, Min, Max, WithScores]));

zrangebyscore(Conn, Key, Min, Max, Limit) when is_pid(Conn),
					       is_integer(Min); is_float(Min); is_list(Min),
					       is_integer(Max); is_float(Max); is_list(Max),
					       is_integer(Limit) ->
    reddy_conn:sync(Conn, ?ZRANGEBYSCORE, [Key, Min, Max, "LIMIT", Limit]);
zrangebyscore(Pool, Key, Min, Max, Limit) when is_atom(Pool),
					       is_integer(Min); is_float(Min); is_list(Min),
					       is_integer(Max); is_float(Max); is_list(Max),
					       is_integer(Limit) ->
    ?WITH_POOL(Pool, ?ZRANGEBYSCORE, [Key, Min, Max, "LIMIT", Limit]).

zrangebyscore(Conn, Key, Min, Max) when is_pid(Conn),
					is_integer(Min); is_float(Min); is_list(Min),
					is_integer(Max); is_float(Max); is_list(Max) ->
    reddy_conn:sync(Conn, ?ZRANGEBYSCORE, [Key, Min, Max]);
zrangebyscore(Pool, Key, Min, Max) when is_atom(Pool),
					is_integer(Min); is_float(Min); is_list(Min),
					is_integer(Max); is_float(Max); is_list(Max) ->
    ?WITH_POOL(Pool, ?ZRANGEBYSCORE, [Key, Min, Max]).
