% I used this for part of the Elastic Replication experiments. I'm just keeping
% it here for documentation purposes
-module(basho_bench_driver_rets).

-export([
      new/1,
      run/4
   ]).

-define(TIMEOUT, 1000).

-define(OPS_TO_REFRESH, 1000).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
   code:add_path("/usr/er/rets/ebin/"),

   Hosts = basho_bench_config:get(rets_hosts),
   MyNode  = basho_bench_config:get(rets_mynode, [basho_bench, longnames]),
%   EtsOpts = basho_bench_config:get(rets_etsopts, []),
   RepProtocol = basho_bench_config:get(rets_protocol),
   RefreshConf = basho_bench_config:get(rets_refresh_conf, false),
   NumRetsNodes = basho_bench_config:get(rets_num_nodes, none),

   EtsOpts = [
      {ram_file, true},
      {min_no_slots, 1000}
   ],

   RouteFn = case basho_bench_config:get(rets_route_to_first, false) of
      true -> fun rets:route_to_first/2;
      false -> fun rets:route/2
   end,

   RepOpts = [ {routefn, RouteFn} | basho_bench_config:get(rets_repopts, []) ],

   % Try to spin up net_kernel
   case net_kernel:start(MyNode) of
      {ok, _} ->
         ?INFO("Net kernel started as ~p\n", [node()]);
      {error, {already_started, _}} ->
         ok;
      {error, NKReason} ->
         ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, NKReason])
   end,

   AllNodes = [ N || {ok, N} <- [slave:start(H, rets,
            "-pa /usr/er/nuwa/ebin /usr/er/repobj/ebin /usr/er/rets/ebin") || H <- Hosts] ],

   RetsNodes = case NumRetsNodes of
      none -> AllNodes;
      N -> lists:sublist(AllNodes, N)
   end,

   Conf = rets:start(EtsOpts, RepProtocol, RetsNodes, RepOpts),
   if
      Id == 1 -> ?CONSOLE("Worker ~p using conf ~p\n", [Id, Conf]);
      true -> never_mind
   end,

   random:seed(now()),     % for protocols that pick random replicas
   State = case RefreshConf of
      true -> {Id, Id, Conf};
      false -> {Id, Conf}
   end,
   {ok, State}.


add_partitions(_, _, [], _, _, _) ->
   done;
add_partitions(Conf, Period, [PNodes | Tail], EtsOpts, EROpts, Retry) ->
   NewConf = elastic:add_partition(Conf, {rets, EtsOpts}, EROpts, PNodes, Retry),
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   timer:sleep(Period),
   add_partitions(NewConf, Period, Tail, EtsOpts, EROpts, Retry).

rm_partitions(_, _, 0, _) ->
   done;
rm_partitions(Conf, Period, N, Retry) ->
   NewConf = elastic:remove_partition(Conf, 1, Retry),
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   timer:sleep(Period),
   rm_partitions(NewConf, Period, N - 1, Retry).

add_replicas(_, _, [], _,  _, _) ->
   done;
add_replicas(Conf, Period, [Nodes | Tail], SeqIndex, EtsOpts, Retry) ->
   NewConf = elastic:add_replicas(Conf, SeqIndex, {rets, EtsOpts}, Nodes, Retry),
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   timer:sleep(Period),
   add_replicas(NewConf, Period, Tail, SeqIndex, EtsOpts, Retry).

rm_replicas(_, _, _, 0, _) ->
   done;
rm_replicas(Conf, Period, SeqIndex, N, Retry) ->
   NewConf = elastic:rm_replicas(Conf, SeqIndex, 1, Retry),
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   timer:sleep(Period),
   rm_replicas(NewConf, Period, SeqIndex, N - 1, Retry).

split(_, _, [], _, _, _) ->
   done;
split(Conf, Period, [{SeqIndex, Nodes}|Tail], EtsOpts, EROpts, Retry) ->
   NewConf = elastic:split_succ(Conf, SeqIndex, {rets, EtsOpts}, EROpts, Nodes, Retry),
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   timer:sleep(Period),
   split(NewConf, Period, Tail, EtsOpts, EROpts, Retry).

merge(_, _, [], _) ->
   done;
merge(Conf, Period, [SeqIndex | Indices], Retry) ->
   NewConf = elastic:merge_succs(Conf, SeqIndex, Retry),
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   timer:sleep(Period),
   merge(NewConf, Period, Indices, Retry).

fail(_, _, []) ->
   done;
fail(Conf, Period, [I | Indices]) ->
   Pid = lists:last( repobj:pids(lists:nth(I, repobj:pids(Conf))) ),
   exit(Pid, fail),
   timer:sleep(Period),
   rets_tracker ! {get_conf, self()},
   NewConf = receive
      {rets_conf, C} -> C
   end,
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   fail(NewConf, Period, Indices).

multi_fail(_, _, []) ->
   done;
multi_fail(Conf, Period, [Indices | Tail]) ->
   Pids = [lists:last(repobj:pids(lists:nth(I, repobj:pids(Conf)))) || I<-Indices],
   [exit(Pid, fail) || Pid <- Pids],
   timer:sleep(Period),
   rets_tracker ! {get_conf, self()},
   NewConf = receive
      {rets_conf, C} -> C
   end,
   ?CONSOLE("New Conf = ~p\n", [NewConf]),
   multi_fail(NewConf, Period, Tail).


% the worker with Id '1' is used to handle "special tasks" like triggering
% failure, repartitioning, reconfiguration ...etc
run(_, _, _, {1, Conf}) ->
   case basho_bench_config:get(special_task, none) of
      none ->
         {stop, "No Special Task"};

      {StartTime, Period, Task} ->
         timer:sleep(StartTime * 1000),
         Retry = basho_bench_config:get(repobj_timeout, ?TIMEOUT),
         case Task of
            {add_partitions, PartitionsNodes} ->
               EtsOpts = basho_bench_config:get(rets_etsopts, []),
               EROpts = basho_bench_config:get(rets_repopts, []),
               add_partitions(Conf, Period * 1000, PartitionsNodes, EtsOpts, EROpts, Retry);

            {rm_partitions, NumPartitions} ->
               rm_partitions(Conf, Period * 1000, NumPartitions, Retry);

            {add_replicas, SeqIndex, Nodes} ->  % SeqIndex = SequencerIndex
               EtsOpts = basho_bench_config:get(rets_etsopts, []),
               add_replicas(Conf, Period * 1000, Nodes, SeqIndex, EtsOpts, Retry);

            {rm_replicas, SeqIndex, NumRmReplicas} ->
               rm_replicas(Conf, Period * 1000, SeqIndex, NumRmReplicas, Retry);

            {split, IndicesPlusNodes} ->
               EtsOpts = basho_bench_config:get(rets_etsopts, []),
               EROpts = basho_bench_config:get(rets_repopts, []),
               split(Conf, Period * 1000, IndicesPlusNodes, EtsOpts, EROpts, Retry);

            {merge, SeqIndices} ->
               merge(Conf, Period * 1000, SeqIndices, Retry);

            {fail, PartitionIndices} ->
               fail(Conf, Period * 1000, PartitionIndices);

            {multi_fail, PartitionIndices} ->
               multi_fail(Conf, Period * 1000, PartitionIndices);

            _ ->
               ?WARN("Unknown special task\n", []),
               ignore
         end,
         {stop, "Done with special task"}
   end;

% refresh configuration by contacting tracker
run(Op, KeyGen, ValueGen, {Id, 0, _Conf}) ->
   rets_tracker ! {get_conf, self()},
   receive
      {rets_conf, NewConf} ->
         run(Op, KeyGen, ValueGen, {Id, ?OPS_TO_REFRESH, NewConf})
   end;

% run the operation and decrement the conf refresh count
run(Op, KeyGen, ValueGen, {Id, OpsTilRefresh, Conf}) ->
   case run(Op, KeyGen, ValueGen, {Id, Conf}) of
      {ok, {Id, NextConf}} ->
         {ok, {Id, OpsTilRefresh - 1, NextConf}};
      {error, Reason, {Id, NextConf}} ->
         {error, Reason, {Id, OpsTilRefresh - 1, NextConf}};
      Other ->
         Other
   end;

% 'read ping' operation to test maximum number of read requests that a service
% can handle
run(rping, KeyGen, _, State = {_, Conf}) ->
   case rets:ping(Conf, rping, KeyGen()) of
      {error, _} ->
         {error, bad_response, State};
      _ ->
         {ok, State}
   end;

% 'write ping' operation to test maximum number of write requests that a service
% can handle
run(wping, KeyGen, _, State = {_, Conf}) ->
   case rets:ping(Conf, wping, KeyGen()) of
      {error, _} ->
         {error, bad_response, State};
      _ ->
         {ok, State}
   end;

% read the value associated with a particular key
run(read, KeyGen, _, State = {_, Conf}) ->
   case rets:read(Conf, KeyGen()) of
      {error, _} ->
         {error, bad_response, State};
      _ ->
         {ok, State}
   end;

% write a value to a particular key location
run(write, KeyGen, ValueGen, State = {Id, Conf}) ->
   Key = KeyGen(),
   Value = ValueGen(),
   case rets:write(Conf, Key, Value) of
      {error, {reconfigured, NewPartitionConf}} ->
         AllPartitions = repobj:pids(Conf),
         OldPartitionConf = rets:route({write, Key}, AllPartitions),
         NewConf = repobj:set_pids(Conf,
            [NewPartitionConf | lists:delete(OldPartitionConf, AllPartitions)]),

         % XXX: handle if this fails again
         rets:write(NewConf, Key, Value),
         {error, reconfigured, {Id, NewConf}};

      {error, {wedged, _}} ->
         {error, wedged, State};

      {error, pending} ->
         {error, pending, State};

      {error, _} ->
         {error, bad_response, State};
      _ ->
         {ok, State}
   end;

% 'output throughput' operation to test maximum throughput out of the driver
run(out_tp, KeyGen, ValueGen, State = {_, Conf}) ->
   Conf ! {KeyGen(), ValueGen()},
   {ok, State}.
