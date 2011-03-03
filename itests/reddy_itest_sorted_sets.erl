-module(reddy_itest_sorted_sets).
-author('Bradford Winfrey <bradford.winfrey@gmail.com>').

-include_lib("eunit/include/eunit.hrl").
-include("reddy_itests.hrl").
-include("../src/reddy.hrl").

-define(TEST_MOD, reddy_sorted_sets).

zadd_test() ->
  {ok, C} = ?CONNECT(),
  {TestKey, TestScore, TestMember} = {<<"zadd_test">>, 42, <<"one">>},
  ?TEST_MOD:zrem(C, TestKey, TestMember),
  ?assertMatch(1, ?TEST_MOD:zadd(C, TestKey, TestScore, TestMember)),
  ?assertMatch([<<"one">>], ?TEST_MOD:zrange(C, TestKey, 0, -1)),
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
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(2, ?TEST_MOD:zcard(C, TestKey)),
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
  ?assertMatch(0, ?TEST_MOD:zrevrank(C, TestKey, <<"not_exist">>)),
  reddy_conn:close(C).

zrange_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zrange_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch([<<"one">>,<<"1">>,<<"two">>,<<"2">>,<<"three">>,<<"3">>], ?TEST_MOD:zrange(C, TestKey, -0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zrevrange_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zrevrange_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch([<<"three">>,<<"3">>,<<"two">>,<<"2">>,<<"one">>,<<"1">>], ?TEST_MOD:zrevrange(C, TestKey, -0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zremrangebyrank_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zremrangebyrank_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(2, ?TEST_MOD:zremrangebyrank(C, TestKey, 0, 1)),
  ?assertMatch([<<"three">>,<<"3">>], ?TEST_MOD:zrange(C, TestKey, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zscore_test() ->
  {ok, C} = ?CONNECT(),
  {TestKey, TestScore, TestMember} = {<<"zscore_test">>, 42, <<"fourty-two">>},
  ?TEST_MOD:zrem(C, TestKey, TestMember),
  ?assertMatch(1, ?TEST_MOD:zadd(C, TestKey, TestScore, TestMember)),
  ?assertMatch(<<"42">>, ?TEST_MOD:zscore(C, TestKey, TestMember)),
  reddy_conn:close(C).

zremrangebyscore_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zremrangebyscore_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch(2, ?TEST_MOD:zremrangebyscore(C, TestKey, <<"-inf">>, 2)),
  ?assertMatch([<<"three">>,<<"3">>], ?TEST_MOD:zrange(C, TestKey, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zrangebyscore_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zrangebyscore_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch([<<"one">>,<<"two">>,<<"three">>], ?TEST_MOD:zrangebyscore(C, TestKey, <<"-inf">>, <<"+inf">>)),
  Opt1 = #reddy_optional_args{withscores=true},
  ?assertMatch([<<"one">>,<<"1">>,<<"two">>,<<"2">>,<<"three">>,<<"3">>], ?TEST_MOD:zrangebyscore(C, TestKey, <<"-inf">>, <<"+inf">>, Opt1)),
  Opt2 = #reddy_optional_args{limit=[0,1]},
  ?assertMatch([<<"one">>], ?TEST_MOD:zrangebyscore(C, TestKey, 0, 1, Opt2)),
  reddy_conn:close(C).

zrevrangebyscore_test() ->
  {ok, C} = ?CONNECT(),
  TestKey = <<"zrevrangebyscore_test">>,
  TestInput = [{TestKey, 1, <<"one">>}, 
                {TestKey, 2, <<"two">>},
                {TestKey, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput],
  ?assertMatch([<<"three">>,<<"two">>,<<"one">>], ?TEST_MOD:zrevrangebyscore(C, TestKey, <<"+inf">>, <<"-inf">>)),
  Opt1 = #reddy_optional_args{withscores=true},
  ?assertMatch([<<"three">>,<<"3">>,<<"two">>,<<"2">>,<<"one">>,<<"1">>], ?TEST_MOD:zrevrangebyscore(C, TestKey, <<"+inf">>, <<"-inf">>, Opt1)),
  Opt2 = #reddy_optional_args{limit=[0,1]},
  ?assertMatch([<<"one">>], ?TEST_MOD:zrevrangebyscore(C, TestKey, 1, 0, Opt2)),
  reddy_conn:close(C).

zinterstore_single_key_test() ->
  {ok, C} = ?CONNECT(),
  TestOut1 = <<"zinterstore_out1">>,
  TestOut2 = <<"zinterstore_out2">>,
  TestKey1 = <<"zinterstore1">>,
  TestKey2 = <<"zinterstore2">>,
  TestInput1 = [{TestKey1, 1, <<"one">>}, 
                {TestKey1, 2, <<"two">>},
                {TestKey1, 3, <<"three">>}],
  TestInput2 = [{TestKey2, 1, <<"one">>}, 
                {TestKey2, 2, <<"two">>},
                {TestKey2, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput1 ++ TestInput2],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput1 ++ TestInput2],
  ?assertMatch(3, ?TEST_MOD:zinterstore(C, TestOut1, TestKey1)),
  ?assertMatch([<<"one">>,<<"1">>,<<"two">>,<<"2">>,<<"three">>,<<"3">>], ?TEST_MOD:zrange(C, TestOut1, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zinterstore_single_key_with_options_test() ->
  {ok, C} = ?CONNECT(),
  TestOut1 = <<"zinterstoreopt_out1">>,
  TestOut2 = <<"zinterstoreopt_out2">>,
  TestKey1 = <<"zinterstoreopt1">>,
  TestKey2 = <<"zinterstoreopt2">>,
  TestInput1 = [{TestKey1, 1, <<"one">>}, 
                {TestKey1, 2, <<"two">>},
                {TestKey1, 3, <<"three">>}],
  TestInput2 = [{TestKey2, 1, <<"one">>}, 
                {TestKey2, 2, <<"two">>},
                {TestKey2, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- [TestOut1, TestOut2]],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput1 ++ TestInput2],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput1 ++ TestInput2],
  ?assertMatch(3, ?TEST_MOD:zinterstore(C, TestOut1, TestKey1, #reddy_optional_args{weights=1})),
  ?assertMatch([<<"one">>,<<"1">>,<<"two">>,<<"2">>,<<"three">>,<<"3">>], ?TEST_MOD:zrange(C, TestOut1, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zinterstore_multi_key_test() ->
  {ok, C} = ?CONNECT(),
  TestOut1 = <<"zinterstore_out1">>,
  TestOut2 = <<"zinterstore_out2">>,
  TestKey1 = <<"zinterstore1">>,
  TestKey2 = <<"zinterstore2">>,
  TestInput1 = [{TestKey1, 1, <<"one">>},
                {TestKey1, 2, <<"two">>},
                {TestKey1, 3, <<"three">>}],
  TestInput2 = [{TestKey2, 1, <<"one">>},
                {TestKey2, 2, <<"two">>},
                {TestKey2, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput1 ++ TestInput2],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput1 ++ TestInput2],
  ?assertMatch(3, ?TEST_MOD:zinterstore(C, TestOut1, #reddy_optional_args{keys=[TestKey1, TestKey2]})),
  ?assertMatch([<<"one">>,<<"2">>,<<"two">>,<<"4">>,<<"three">>,<<"6">>], ?TEST_MOD:zrange(C, TestOut1, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zinterstore_multi_key_with_options_test() ->
  {ok, C} = ?CONNECT(),
  TestOut1 = <<"zinterstore_out1">>,
  TestOut2 = <<"zinterstore_out2">>,
  TestKey1 = <<"zinterstore1">>,
  TestKey2 = <<"zinterstore2">>,
  TestInput1 = [{TestKey1, 1, <<"one">>},
                {TestKey1, 2, <<"two">>},
                {TestKey1, 3, <<"three">>}],
  TestInput2 = [{TestKey2, 1, <<"one">>},
                {TestKey2, 2, <<"two">>},
                {TestKey2, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput1 ++ TestInput2],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput1 ++ TestInput2],
  ?assertMatch(3, ?TEST_MOD:zinterstore(C, TestOut1, #reddy_optional_args{keys=[TestKey1, TestKey2],weights=[2,3]})),
  ?assertMatch([<<"one">>,<<"2">>,<<"two">>,<<"4">>,<<"three">>,<<"6">>], ?TEST_MOD:zrange(C, TestOut1, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).

zunionstore_test() ->
  {ok, C} = ?CONNECT(),
  TestKeyOut = <<"zunionout">>,
  TestKey1 = <<"zunion1">>,
  TestKey2 = <<"zunion2">>,
  TestInput1 = [{TestKey1, 1, <<"one">>},
                {TestKey1, 2, <<"two">>}],
  TestInput2 = [{TestKey2, 1, <<"one">>},
                {TestKey2, 2, <<"two">>},
                {TestKey2, 3, <<"three">>}],
  [?TEST_MOD:zrem(C, Key, Member) || {Key, _Score, Member} <- TestInput1 ++ TestInput2],
  [?TEST_MOD:zadd(C, Key, Score, Member) || {Key, Score, Member} <- TestInput1 ++ TestInput2],
  ?assertMatch(3, ?TEST_MOD:zunionstore(C, TestKeyOut, #reddy_optional_args{keys=[TestKey1, TestKey2],weights=[2,3]})),
  ?assertMatch([<<"one">>,<<"2">>,<<"three">>,<<"3">>,<<"two">>,<<"4">>], ?TEST_MOD:zrange(C, TestKeyOut, 0, -1, #reddy_optional_args{withscores=true})),
  reddy_conn:close(C).
