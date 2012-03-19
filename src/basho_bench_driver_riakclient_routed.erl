%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_riakclient_routed).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { clients,
                 bucket,
                 replies,
                 target_node,
                 default_client,
                 ring
              }).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riak_client) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_client module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Nodes   = basho_bench_config:get(riakclient_nodes),
    Cookie  = basho_bench_config:get(riakclient_cookie, 'riak'),
    MyNode  = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    Replies = basho_bench_config:get(riakclient_replies, 2),
    Bucket  = basho_bench_config:get(riakclient_bucket, <<"test">>),

    %% Try to spin up net_kernel
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    % Set the local cookie
    erlang:set_cookie(node(), Cookie),

    %% Try to ping each of the nodes
    ping_each(Nodes),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    {ok, Ring} = rpc:call(TargetNode, riak_core_ring_manager, get_my_ring, []),
    AllNodes = riak_routing:all_members(Ring),

    case riak:client_connect(TargetNode) of
       {ok, DefaultClient} ->
          Clients = connect_to_nodes(AllNodes, dict:store(TargetNode, DefaultClient, dict:new())),
          case dict:size(Clients) of
             0 ->
                ?FAIL_MSG("Failed to connect to any node\n", []);
             _ ->
                {ok, #state {
                      clients = Clients,
                      bucket  = Bucket,
                      replies = Replies,
                      target_node = TargetNode,
                      default_client = DefaultClient,
                      ring = Ring
                   }}
       end;
    {error, Reason2} ->
          ?FAIL_MSG("Failed to get a riak:client_connect to ~p: ~p\n", [TargetNode, Reason2])
    end.

run(get, KeyGen, _ValueGen, State=#state{bucket=Bucket, replies=Replies}) ->
    Key = KeyGen(),
    Client = get_client(Bucket, Key, State),
    case Client:get(Bucket, Key, Replies) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State=#state{bucket=Bucket, replies=Replies}) ->
    Key = KeyGen(),
    Robj = riak_object:new(Bucket, Key, ValueGen()),
    Client = get_client(Bucket, Key, State),
    case Client:put(Robj, Replies) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State=#state{bucket=Bucket, replies=Replies}) ->
    Key = KeyGen(),
    Client = get_client(Bucket, Key, State),
    case Client:get(Bucket, Key, Replies) of
        {ok, Robj} ->
            Robj2 = riak_object:update_value(Robj, ValueGen()),
            case Client:put(Robj2, Replies) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj = riak_object:new(Bucket, Key, ValueGen()),
            case Client:put(Robj, Replies) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;
run(delete, KeyGen, _ValueGen, State=#state{bucket=Bucket, replies=Replies}) ->
    Key = KeyGen(),
    Client = get_client(Bucket, Key, State),
    case Client:delete(Bucket, Key, Replies) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run(refresh_ring, _KeyGen, _ValueGen, State) ->
   {ok, Ring} = rpc:call( State#state.target_node, riak_core_ring_manager, get_my_ring, []),
   AllNodes = riak_routing:all_members(Ring),
   Clients = connect_to_nodes(AllNodes, State#state.clients),
   {ok, State#state{ring = Ring, clients = Clients}}.



%% ====================================================================
%% Internal functions
%% ====================================================================

ping_each([]) ->
    ok;
ping_each([Node | Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            ping_each(Rest);
        pang ->
            ?FAIL_MSG("Failed to ping node ~p\n", [Node])
    end.

get_client(Bucket, Key, State) ->
   % Get the consistent hashing ring state
   {_, _NodeNam, _VClock, Chring, _Meta, _ClusterName, _Next, _Members,
      _Claimant, _Seen, _RVSN} = State#state.ring,

   % hash the bucket/key to get a ring index
   Index = riak_routing:hash({Bucket, Key}),

   % find the home node for the given index
   [{_PartitionNum, HomeNode}] = riak_routing:successors(Index, Chring, 1),

   % Return the riak_client for the given home_node, if not found, return
   % default client
   case dict:find(HomeNode, State#state.clients) of
      {ok, Client} ->
         Client;
      error ->
         State#state.default_client
   end.

connect_to_nodes([], Clients) ->
   Clients;
connect_to_nodes([Head | Tail], Clients) ->
   case dict:is_key(Head, Clients) of
      true ->
         connect_to_nodes(Tail, Clients);
      false ->
         case riak:client_connect(Head) of
            {ok, Client} ->
               connect_to_nodes(Tail, dict:store(Head, Client, Clients));
            {error, Reason} ->
               ?ERROR("Failed get a riak:client_connect to ~p: ~p\n", [Head, Reason])
         end
   end.

