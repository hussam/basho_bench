-module(basho_bench_driver_rets).

-export([
      new/1,
      run/4
   ]).

-define(TIMEOUT, 1000).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

conf_server(Conf) ->
   receive
      {Worker, get_conf} -> Worker ! Conf, conf_server(Conf)
   end.

new(Id) ->
   code:add_path("/usr/er/rets/ebin/"),

   Hosts = basho_bench_config:get(rets_hosts),
   MyNode  = basho_bench_config:get(rets_mynode, [basho_bench, longnames]),
   Spec = basho_bench_config:get(rets_spec),

   % Try to spin up net_kernel
   case net_kernel:start(MyNode) of
      {ok, _} ->
         ?INFO("Net kernel started as ~p\n", [node()]);
      {error, {already_started, _}} ->
         ok;
      {error, NKReason} ->
         ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, NKReason])
   end,

   [ N || {ok, N} <- [slave:start(H, rets,
            "-pa /usr/er/libdist/ebin /usr/er/rets/ebin") || H <- Hosts] ],

   % for protocols that pick random replicas
   random:seed(now()),
   [ random:uniform(1000) || _ <- lists:seq(1, Id) ],

   if
      Id == 1 ->
         Conf = libdist:build(Spec),
         register(conf_server, spawn(fun() -> conf_server(Conf) end)),
         ?CONSOLE("Worker ~p using conf ~p\n", [Id, Conf]),
         {ok, Conf};
      true ->
         conf_server ! {self(), get_conf},
         receive
            Conf -> {ok, Conf}
         end
   end.


% 'read ping' operation to test maximum number of read requests that a service
% can handle
run(rping, KeyGen, _, Conf) ->
   case rets:ping(Conf, rping, KeyGen()) of
      {error, Err} ->
         {error, {bad_response, Err}, Conf};
      _ ->
         {ok, Conf}
   end;

% 'write ping' operation to test maximum number of write requests that a service
% can handle
run(wping, KeyGen, _, Conf) ->
   case rets:ping(Conf, wping, KeyGen()) of
      {error, Err} ->
         {error, {bad_response, Err}, Conf};
      _ ->
         {ok, Conf}
   end;

% read the value associated with a particular key
run(read, KeyGen, _, Conf) ->
   case rets:read(Conf, KeyGen()) of
      {error, Err} ->
         {error, {bad_response, Err}, Conf};
      _ ->
         {ok, Conf}
   end;

% write a value to a particular key location
run(write, KeyGen, ValueGen, Conf) ->
   Key = KeyGen(),
   Value = ValueGen(),
   case rets:write(Conf, Key, Value) of
      {error, Err} ->
         {error, {bad_response, Err}, Conf};
      _ ->
         {ok, Conf}
   end.
