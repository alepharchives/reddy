-module(reddy_itest_sorted_sets).
-author('Bradford Winfrey <bradford.winfrey@gmail.com>').

-include_lib("eunit/include/eunit.hrl").
-include("reddy_itests.hrl").
-define(TEST_MOD, reddy_sorted_sets).

zadd_test() ->
  {ok, C} = ?CONNECT(),
  {TestKey, TestScore, TestMember} = {<<"zadd_test">>, 42, <<"one">>},
  ?TEST_MOD:zrem(C, TestKey, TestMember),
  ?assertMatch(1, ?TEST_MOD:zadd(C, TestKey, TestScore, TestMember)),
  % ?assertMatch(TestMember, ?TEST_MOD:zrange(TestKey, 0, -1)),
  ?assertMatch(1,?TEST_MOD:zrem(C, TestKey, TestMember)),
  reddy_conn:close(C).

zrem_test() ->
  {ok, C} = ?CONNECT(),
  {TestKey, TestScore, TestMember} = {<<"zadd_test">>, 42, <<"one">>},
  ?TEST_MOD:zadd(C, TestKey, TestScore, TestMember),
  ?assertMatch(1, ?TEST_MOD:zrem(C, TestKey, TestMember)),
  reddy_conn:close(C).

zcard_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zcard_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, {TestKey, 2, <<"two">>}],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(2, ?TEST_MOD:zcard(C, TestKey)),
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  reddy_conn:close(C).

zcount_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zcount_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>}, 
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(3, ?TEST_MOD:zcount(C, TestKey, <<"-inf">>, <<"+inf">>)),
  ?assertMatch(2, ?TEST_MOD:zcount(C, TestKey, <<"(1">>, <<"3">>)),
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  reddy_conn:close(C).

zincrby_test() ->
  {ok, C} = ?CONNECT(),
  {TestKey, TestMember} = {<<"zincrby_test">>, <<"test">>},
  ?TEST_MOD:zrem(C, TestKey, TestMember),
  ?TEST_MOD:zadd(C, TestKey, 1, TestMember),
  ?assertMatch(<<"2">>, ?TEST_MOD:zincrby(C, TestKey, 1, TestMember)),
  ?assertMatch(<<"3">>, ?TEST_MOD:zincrby(C, TestKey, 1, TestMember)),
  ?assertMatch(<<"4">>, ?TEST_MOD:zincrby(C, TestKey, 1, TestMember)),
  ?assertMatch(<<"3">>, ?TEST_MOD:zincrby(C, TestKey, -1, TestMember)),
  ?assertMatch(<<"4">>, ?TEST_MOD:zincrby(C, TestKey, 1, TestMember)),
  ?TEST_MOD:zrem(C, TestKey, TestMember),
  reddy_conn:close(C).

zrank_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zrank_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(2, ?TEST_MOD:zrank(C, TestKey, <<"three">>)),
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  reddy_conn:close(C).

zrevrank_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zrevrank_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(2, ?TEST_MOD:zrevrank(C, TestKey, <<"one">>)),
  ?assertMatch(-1, ?TEST_MOD:zrevrank(C, TestKey, <<"not_exist">>)),
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  reddy_conn:close(C).
