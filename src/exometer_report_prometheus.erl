-module(exometer_report_prometheus).

-behaviour(exometer_report).

-export([fetch/0]).

-export([
    exometer_init/1,
    exometer_info/2,
    exometer_cast/2,
    exometer_call/3,
    exometer_report/5,
    exometer_subscribe/5,
    exometer_unsubscribe/4,
    exometer_newentry/2,
    exometer_setopts/4,
    exometer_terminate/2,
    make_metric_name/2,
    fetch_and_format_metrics/1
]).


-record(state, {entries = #{} :: list()}).

%% -------------------------------------------------------
%% Public API
%% -------------------------------------------------------

fetch() ->
    exometer_report:call_reporter(exometer_report_prometheus, {request, fetch}).


%% -------------------------------------------------------
%% exometer reporter callbacks
%% -------------------------------------------------------

exometer_init(Opts) ->
    case lists:member(enable_httpd, Opts) of
        true -> exometer_prometheus_httpd:start(Opts);
        false -> ok
    end,
    {ok, #state{}}.

exometer_subscribe(Metric, _DataPoints, _Interval, Opts, State = #state{entries=Entries}) ->

    FieldMap = proplists:get_value(fieldmap, Opts, node),
    {Name, Labels} = make_metric_name(Metric, FieldMap),
    Type = exometer:info(Metric, type),
    Help = proplists:get_value(help, Opts, <<"undefined">>),
    case maps:get(Name,Entries,new) of
        new ->
            LabelMap = #{Labels => Metric},
            {ok, State#state{entries = Entries#{Name => {Type, Help, LabelMap}}}};
        {CurrentType, CurrentHelp, LabelMap}->
            case Type of
                CurrentType ->
                    % Entry = {Metric},
                    % #{Name := {CurrentHelp, CurrentType, LabelMap}} = Entries,
                    NewLabelMap = LabelMap#{Labels => Metric},
                    {ok, State#state{entries = Entries#{Name => {CurrentType, CurrentHelp, NewLabelMap}}}};
            _ ->
                {error, "Invalid type"}
        end
    end.

exometer_unsubscribe(Metric, _DataPoints, _Extra, State = #state{entries = Entries}) ->
    {ok, State#state{entries = proplists:delete(Metric, Entries)}}.

exometer_call({request, fetch}, _From, State = #state{entries = Entries}) ->
    {reply, fetch_and_format_metrics(maps:to_list(Entries)), State};
exometer_call(_Req, _From, State) ->
    {ok, State}.

exometer_newentry(Entry, State) -> 
    S = {exometer_entry,
                  [riak,riak_core,vnodeq,gate_queue_vnode,
                   274031556999544297163190906134303066185487351808],
                  function,undefined,exometer_function,1,0,undefined,
                  undefined,
                  [{arg,
                       {erlang,process_info,
                           ["<0.4597.0>",message_queue_len],
                           match,
                           {'_',value}}}],
                  undefined},
    case element(2,Entry) of
        [riak,riak_core,vnodeq,_,_] = Metric ->
            exometer_subscribe(Metric, [value], 0, [{help, <<"Vnode queue">>},{fieldmap,[ignore,name,name,vnode_type,partition]}],State);
        [riak,riak_core,dropped_vnode_requests] = Metric ->
            exometer_subscribe(Metric, [value], 0, [{help, <<"Dropped vnode requests">>},{fieldmap,[ignore,name,name]}],State);
        _ ->
            {ok, State}
    end.
exometer_report(_Metric, _DataPoint, _Extra, _Value, State) -> 
    io:format("REPORT --> ~p, ~p, ~p, ~p",[_Metric, _DataPoint, _Extra, _Value]),
    {ok, State}.
exometer_cast(_Unknown, State) -> {ok, State}.

exometer_info(_Info, State) -> {ok, State}.
exometer_setopts(_Metric, _Options, _Status, State) -> {ok, State}.
exometer_terminate(_Reason, _) -> ignore.


%% -------------------------------------------------------
%% internal
%% -------------------------------------------------------

fetch_and_format_metrics(Entries) ->
    Metrics = fetch_metrics(Entries,[]),
    format_metrics(Metrics).

% fetch_metrics(Entries) ->
%     fetch_metrics(Entries, []).

fetch_metrics([], Akk) ->
    Akk;
fetch_metrics([{Name, {Type, Help, LabelMetricEntries}} | Entries], Acc) ->
    LabelMetricValues = fetch_label_metrics(Type, maps:to_list(LabelMetricEntries),[]),
    fetch_metrics(Entries, [{Name, Type, Help, LabelMetricValues} | Acc]).

fetch_label_metrics(_Type, [], Acc) ->
    Acc;
fetch_label_metrics(counter, [{Labels,Metric} | LabelEntries], Acc) ->
    case exometer:get_value(Metric,[value]) of
        {ok, DataPointValues} ->
            fetch_label_metrics(counter, LabelEntries, [{Labels, DataPointValues} | Acc]);
        _Error ->
            fetch_label_metrics(counter, LabelEntries, Acc)
    end;
fetch_label_metrics(gauge, [{Labels,Metric} | LabelEntries], Acc) ->
    case exometer:get_value(Metric,[value]) of
        {ok, DataPointValues} ->
            fetch_label_metrics(gauge, LabelEntries, [{Labels, DataPointValues} | Acc]);
        _Error ->
            fetch_label_metrics(gauge, LabelEntries, Acc)
    end;
fetch_label_metrics(Type, [{Labels,Metric} | LabelEntries], Acc) ->
    case exometer:get_value(Metric) of
        {ok, DataPointValues} ->
            fetch_label_metrics(Type, LabelEntries, [{Labels, DataPointValues} | Acc]);
        _Error ->
            fetch_label_metrics(Type, LabelEntries, Acc)
    end.

format_metrics(Metrics) ->
    Formatted = format_metrics(Metrics, []),
    iolist_to_binary(Formatted).

format_metrics([], Akk) ->
    Akk;
format_metrics([{Name, Type, Help, LabelMetrics}|Rest],Acc) ->
    Payload = [[<<"# HELP ">>, Name, <<" ">>, Help, <<"\n">>,
                <<"# TYPE ">>, Name, <<" ">>, map_type(Type),<<"\n">>]],
    FormattedLabelMetrics = format_label_metrics(Name, Type, LabelMetrics,[]),
    format_metrics(Rest, [Payload,FormattedLabelMetrics|Acc]).

format_label_metrics(_, _, [], Acc) ->
    Acc;
format_label_metrics(Name, duration, [{Label, [{count,_},{last,_},{n,N},{mean,Mean},{min,_},{max,_},{median,_}|Rest]} | Metrics], Acc) ->
    Buckets = format_duration_bukcets(Name, Label, Rest,[]),
    Payload = [
        Name,<<"_count">>,format_labels(Label,[]),<<" ">>,ioize(N),<<"\n">>,
        Name,<<"_sum">>,format_labels(Label,[]),<<" ">>,ioize(N*Mean),<<"\n">>
    ],
    format_label_metrics(Name, duration, Metrics, [Buckets,Payload|Acc]);
format_label_metrics(Name, histogram, [{Label, [{n,N},{mean,Mean},{min,_},{max,_},{median,_}|Rest]} | Metrics], Acc) ->
    Buckets = format_histogram_bukcets(Name, Label, Rest,[]),
    Payload = [
        Name,<<"_count">>,format_labels(Label,[]),<<" ">>,ioize(N),<<"\n">>,
        Name,<<"_sum">>,format_labels(Label,[]),<<" ">>,ioize(N*Mean),<<"\n">>
    ],
    format_label_metrics(Name, histogram, Metrics, [Buckets,Payload|Acc]);
format_label_metrics(Name, counter, [{Label, [{value, Value}]} | Metrics], Acc) ->
    Payload = [
        Name,format_labels(Label,[]),<<" ">>,ioize(Value),<<"\n">>
    ],
    format_label_metrics(Name, counter, Metrics, [Payload|Acc]);
format_label_metrics(Name, gauge, [{Label, [{value, Value}]} | Metrics], Acc) ->
    Payload = [
        Name,format_labels(Label,[]),<<" ">>,ioize(Value),<<"\n">>
    ],
    format_label_metrics(Name, gauge, Metrics, [Payload|Acc]);
format_label_metrics(Name, function, [{Label, [{value, Value}]} | Metrics], Acc) ->
    Payload = [
        Name,format_labels(Label,[]),<<" ">>,ioize(Value),<<"\n">>
    ],
    format_label_metrics(Name, function, Metrics, [Payload|Acc]).

format_duration_bukcets(_Name,_Label,[],Acc) ->
    Acc;
format_duration_bukcets(Name,Label,[{Bucket, Value}|Rest],Acc) ->
    Payload = [Name, format_labels([{<<"quantile">>,[<<"0.">>,ioize_val(Bucket)]}|Label],[]), <<" ">>, ioize(Value),<<"\n">>],
    format_duration_bukcets(Name, Label, Rest, Acc++Payload).

format_histogram_bukcets(_Name,_Label,[],Acc) ->
    Acc;
format_histogram_bukcets(Name,Label,[{Bucket, Value}|Rest],Acc) ->
    Payload = [Name, format_labels([{<<"quantile">>,[<<"0.">>,ioize_val(Bucket)]}|Label],[]), <<" ">>, ioize(Value),<<"\n">>],
    format_histogram_bukcets(Name, Label, Rest, Acc++Payload).

format_labels([],[]) ->
    [];
format_labels([],Acc) ->
    [<<"{">>, Acc, <<"}">>];
format_labels([{Label,Value}|Rest],[]) ->
    format_labels(Rest,[Label,<<"=\"">>,Value,<<"\"">>]);
format_labels([{Label,Value}|Rest],Acc) ->
    format_labels(Rest,Acc++[<<",">>,Label,<<"=\"">>,Value,<<"\"">>]).

make_metric_name(Metric, []) ->
    NameList = lists:join($_, lists:map(fun ioize/1, Metric)),
    iolist_to_binary(NameList);

make_metric_name(Metric, FieldMap) ->
    make_metric_name(Metric, FieldMap,{<<>>,[]}).
make_metric_name([], [], Name) ->
    Name;
make_metric_name([_MetricH|MetricR], [ignore|FieldR], Name) ->
    make_metric_name(MetricR,FieldR, Name);
make_metric_name([MetricH|MetricR],[name|FieldR], {<<>>, []}) ->
    make_metric_name(MetricR,FieldR, {ioize(MetricH),[]});
make_metric_name([MetricH|MetricR],[name|FieldR],{AccName, Labels}) ->
    Name = ioize(MetricH),
    make_metric_name(MetricR,FieldR, {<<AccName/binary, <<"_">>/binary, Name/binary>>,Labels});
make_metric_name([MetricH|MetricR],[Field|FieldR],{AccName,Labels}) ->
    LabelName = ioize(Field),
    LabelVal = ioize_val(MetricH),
    make_metric_name(MetricR,FieldR,{AccName, Labels++[{LabelName,LabelVal}]}).

ioize(Bin) when is_binary(Bin) ->
    Bin;
ioize(Atom) when is_atom(Atom) ->
    re:replace(atom_to_binary(Atom, utf8), "-|\\.", "_", [global, {return,binary}]);
ioize(Number) when is_float(Number) ->
    float_to_binary(Number, [{decimals, 4}]);
ioize(Number) when is_integer(Number) ->
    integer_to_binary(Number).
    
ioize_val(Bin) when is_binary(Bin) ->
    Bin;
ioize_val(Atom) when is_atom(Atom) ->
    re:replace(atom_to_binary(Atom, utf8), "-|\\.", "_", [global, {return,binary}]);
ioize_val(Number) when is_float(Number) ->
    float_to_binary(Number, [{decimals, 4}]);
ioize_val(Number) when is_integer(Number) ->
    integer_to_binary(Number).

map_type(counter)       -> <<"counter">>;
map_type(function)       -> <<"counter">>;
map_type(gauge)         -> <<"gauge">>;
map_type(histogram)     -> <<"summary">>;
map_type(duration)     -> <<"summary">>.
