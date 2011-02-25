-record(zrange_op, {withscores = <<"WITHSCORES">>}).

-record(zrevrange_op, {withscores = <<"WITHSCORES">>}).

%% Feed withscores with <<"WITHSCORES">>
%% Likewise for limit: <<"LIMIT offset count">>
%% Finally, this seems hacky, need to discuss empty options as
%% they kept parsed as [undefined] by reddy_protocol:encode_arg/1
-record(zrangebyscore_op, {withscores, limit}).