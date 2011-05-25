-module(reddy_itest_server).

-define(TEST_KEY1, <<"reddy_keys1">>).

-include_lib("eunit/include/eunit.hrl").
-include("reddy_itests.hrl").

flushdb_test() ->
    {ok, C} = ?CONNECT(),
    reddy_keys:del(C, [?TEST_KEY1]),
    reddy_lists:lpush(C, ?TEST_KEY1, <<"one">>),
    ?assertMatch(1, reddy_keys:exists(C, ?TEST_KEY1)),
    reddy_server:flushdb(C),
    ?assertMatch(0, reddy_keys:exists(C, ?TEST_KEY1)),
    reddy_conn:close(C).
