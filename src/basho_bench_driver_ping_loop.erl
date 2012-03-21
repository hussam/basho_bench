-module(basho_bench_driver_ping_loop).

-export([
      new/1,
      run/4,
      server_loop/0
   ]).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
   HostName = basho_bench_config:get(ping_server_hostname),
   MyNode  = basho_bench_config:get(ping_mynode, [basho_bench, longnames]),
   Cookie = ping_loop,

   % Try to spin up net_kernel
   case net_kernel:start(MyNode) of
      {ok, _} ->
         ?INFO("Net kernel started as ~p\n", [node()]);
      {error, {already_started, _}} ->
         ok;
      {error, NKReason} ->
         ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, NKReason])
   end,

   % Set the local cookie
   erlang:set_cookie(node(), Cookie),

   case slave:start(HostName, ping_server, "-setcookie " ++ atom_to_list(Cookie)) of
      {ok, Node} ->
         {_Module, Binary, Filename} = code:get_object_code(?MODULE),
         case rpc:call(Node, code, load_binary, [?MODULE, Filename, Binary]) of
            {error, LBReason} ->
               ?FAIL_MSG("Failed to load binary on remote node: ~p\n", [LBReason]);
            _ ->
               ok
         end,
         ServerPid = spawn(Node, fun server_loop/0),
         rpc:call(Node, erlang, register, [server_loop, ServerPid]),
         ?CONSOLE("Server loop started as ~p on ~p\n", [ServerPid, Node]),
         {ok, {self(), {server_loop, Node}}};

      {error, {already_running, Node}} ->
         {ok, {self(), {server_loop, Node}}};

      {error, SReason} ->
         ?FAIL_MSG("Failed to start slave server for ping loop. Reason: ~p\n", [SReason])
   end.

% Messages to the server need not be tagged with a reference because each
% client/worker runs in a tight loop, so no threat of overlapping requests.

run(ping, _KeyGen, _ValueGen, State = {Self, Server}) ->
   Server ! {Self, ping},
   receive _ -> {ok, State} end;

run(get, KeyGen, _ValueGen, State = {Self, Server}) ->
   Server ! {Self, KeyGen()},
   receive _ -> {ok, State} end;

run(put, KeyGen, ValueGen, State = {Self, Server}) ->
   Server ! {Self, KeyGen(), ValueGen()},
   receive _ -> {ok, State} end.


server_loop() ->
   receive
      {Client, _Key} -> Client ! ok;
      {Client, _Key, _Value} -> Client ! ok
   end,
   server_loop().
