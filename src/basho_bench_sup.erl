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
-module(basho_bench_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         clients/0,
         warmers/0,
         warmers_ids/0,
         all_workers/0,
         stop_child/1
      ]).

%% Supervisor callbacks
-export([init/1]).

-include("basho_bench.hrl").

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

clients() ->
    [Pid || {{client, _Id}, Pid, worker, [basho_bench_worker]} <- supervisor:which_children(?MODULE)].

warmers() ->
    [Pid || {{warmer, _Id}, Pid, worker, [basho_bench_worker]} <- supervisor:which_children(?MODULE)].

warmers_ids() ->
    [{warmer, Id} || {{warmer, Id}, _Pid, worker, [basho_bench_worker]} <- supervisor:which_children(?MODULE)].

all_workers() ->
    [Pid || {_, Pid, worker, [basho_bench_worker]} <- supervisor:which_children(?MODULE)].


stop_child(Id) ->
    ok = supervisor:terminate_child(?MODULE, Id),
    ok = supervisor:delete_child(?MODULE, Id).


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
   % Create the child spec for the workers used to warmup the system (if any)
   Warmers = case basho_bench_config:get(warmup, []) of
      [] -> [];
      {NumWarmers, _WarmupDuration} -> worker_specs(warmer, NumWarmers, [])
   end,

   % Create the child spec for the workers used as clients to test the system
   Clients = case basho_bench_config:get(scale_clients, []) of
      [] ->
         NumClients = basho_bench_config:get(concurrent, []),
         worker_specs(client, NumClients, []);

      {_ClientsInc, _Duration, MaxClients} ->
         worker_specs(client, MaxClients, [])
   end,


   % Create the child spec for measurement workers (if any)
   MeasurementDriver = case basho_bench_config:get(measurement_driver, undefined) of
      undefined -> [];
      _Driver -> [?CHILD(basho_bench_measurement, worker)]
   end,

   {ok, {{one_for_one, 5, 10},
         [?CHILD(basho_bench_log, worker)] ++
         [?CHILD(basho_bench_stats, worker)] ++
         Warmers ++
         Clients ++
         MeasurementDriver
      }}.

%% ===================================================================
%% Internal functions
%% ===================================================================

worker_specs(_Kind, 0, Acc) ->
    Acc;
worker_specs(Kind, Count, Acc) ->
    Id = {Kind, list_to_atom(lists:concat(['basho_bench_worker_', Count]))},
    Spec = {Id, {basho_bench_worker, start_link, [Id, Kind, Count]},
            permanent, 5000, worker, [basho_bench_worker]},
    worker_specs(Kind, Count-1, [Spec | Acc]).
