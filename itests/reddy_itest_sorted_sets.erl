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
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, with_scores)),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two"),
    reddy_conn:close(C).

zrem_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 2}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, with_scores)),
    ?assertMatch(1, reddy_sorted_sets:zrem(C, ?TEST_ZET1, "two")),
    ?assertMatch([{<<"one">>, 1}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, with_scores)),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "one"),
    reddy_sorted_sets:zrem(C, ?TEST_ZET1, "three"),
    reddy_conn:close(C).

zrange_test() ->
    {ok, C} = ?CONNECT(),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 1, "one")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 2, "two")),
    ?assertMatch(1, reddy_sorted_sets:zadd(C, ?TEST_ZET1, 3, "three")),
    ?assertMatch([{<<"one">>, 1}, {<<"two">>, 2}, {<<"three">>, 3}], reddy_sorted_sets:zrange(C, ?TEST_ZET1, 0, -1, with_scores)),
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
