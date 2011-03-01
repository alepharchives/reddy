-record(reddy_op, {name,
                   resp_type,
                   args=[]}).

-define(REDDY_EOL, [13, 10]).

-record(reddy_optional_args, {withscores=false, limit, weights, aggregate}).
-record(reddy_optional_arg, {args=[]}).

-define(WITHSCORES, "WITHSCORES").

-define(LIMIT, "LIMIT").

-define(WEIGHTS, "WEIGHTS").

-define(AGGREGATE, "AGGREGATE").
