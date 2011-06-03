-module(reddy_itest_sorted_sets).

-define(TEST_ZET1, <<"reddy_sorted_set1">>).
-define(TEST_ZET2, <<"reddy_sorted_set2">>).

-include_lib("eunit/include/eunit.hrl").
-include("reddy_itests.hrl").

zadd_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(0, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "two")),
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_conn:close(C).

zrem_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 2}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    ?assertMatch(1, reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two")),
    ?assertMatch([{<<"one">>, 1}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).

zrange_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 2}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    ?assertMatch([<<"one">>, <<"two">>, <<"three">>], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1)),
    ?assertMatch([<<"three">>], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 2, 3)),
    ?assertMatch([<<"two">>, <<"three">>], reddy_sorted_sets:zrange(C, ?TEST_ZET1, -2, -1)),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).
    
zcard_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(2, reddy_sorted_sets:zcard(C, ?TEST_ZET1)),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_conn:close(C).

zcount_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch(3, reddy_sorted_sets:zcount(C, ?TEST_ZET1, "-inf", "+inf")),
    ?assertMatch(2, reddy_sorted_sets:zcount(C, ?TEST_ZET1, "(1", 3)),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).

zincrby_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(3, reddy_sorted_sets:zincrby(C, ?TEST_ZET1, 2, "one")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_conn:close(C).

zrank_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch(2, reddy_sorted_sets:zrank(C, ?TEST_ZET1, "three")),
    ?assertMatch(0, reddy_sorted_sets:zrank(C, ?TEST_ZET1, "four")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).

zremrangebyrank_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 2}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    ?assertMatch(2, reddy_sorted_sets:zremrangebyrank(C, ?TEST_ZET1, 0, 1)),
    ?assertMatch([{<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).

zrevrange_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([{<<"three">>, 3}, {<<"two">>, 2}, {<<"one">>, 1}], reddy_sorted_sets:zrevrange(C, ?TEST_ZET1, 0, -1, "WITHSCORES")),
    ?assertMatch([<<"three">>, <<"two">>, <<"one">>], reddy_sorted_sets:zrevrange(C, ?TEST_ZET1, 0, -1)),
    ?assertMatch([<<"one">>], reddy_sorted_sets:zrevrange(C, ?TEST_ZET1, 2, 3)),
    ?assertMatch([<<"two">>, <<"one">>], reddy_sorted_sets:zrevrange(C, ?TEST_ZET1, -2, -1)),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).

zscore_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zscore(C, ?TEST_ZET1, "one")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_conn:close(C).

%% @todo this test has to be more extensive
zrangebyscore_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([<<"one">>, <<"two">>, <<"three">>], reddy_sorted_sets:zrangebyscore(C, ?TEST_ZET1, "-inf", "+inf")),
    ?assertMatch([<<"one">>, <<"two">>], reddy_sorted_sets:zrangebyscore(C, ?TEST_ZET1, 1, 2)),
    ?assertMatch([<<"two">>], reddy_sorted_sets:zrangebyscore(C, ?TEST_ZET1, "(1", 2)),
    ?assertMatch([], reddy_sorted_sets:zrangebyscore(C, ?TEST_ZET1, "(1", "(2")),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).
