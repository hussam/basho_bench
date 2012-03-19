-module(riak_routing).

-export([all_members/1,
         hash/1,
         successors/3]).

-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space

%% ====================================================================
%% API
%% ====================================================================


%% @doc Produce a list of all nodes that are members of the cluster
all_members(Ring) ->
   % Parse the consistent hashing state
   {_, _NodeNam, _VClock, _Chring, _Meta, _ClusterName, _Next, Members,
      _Claimant, _Seen, _RVSN} = Ring,

   % All member types
   Types = [joining, valid, leaving, exiting, down],

   % Get all the nodes in the ring member list
   [Node || {Node, {V, _, _}} <- Members, lists:member(V, Types)].



%% @doc Given any term used to name an object, produce that object's key
%%      into the ring.  Two names with the same SHA-1 hash value are
%%      considered the same name.
hash({_Bucket, _Key} = ObjectName) ->
   crypto:sha(term_to_binary(ObjectName)).

%% @doc given an object key, return the next n nodeentries in order
%%      starting at index.
successors(Index, CHash, N) ->
   Num = max_n(N, CHash),
   Ordered = ordered_from(Index, CHash),
   {NumPartitions, _Nodes} = CHash,
   if Num =:= NumPartitions ->
         Ordered;
      true ->
         {Res, _} = lists:split(Num, Ordered),
         Res
   end.


%%%====================
%%% Internal functions
%%%====================

%% @doc Return either N or the number of partitions in the ring, whichever
%%      is lesser.
max_n(N, {NumPartitions, _Nodes}) ->
   erlang:min(N, NumPartitions).


%% @doc Given an object key, return all NodeEntries in order starting at Index.
ordered_from(Index, {NumPartitions, Nodes}) ->
   <<IndexAsInt:160/integer>> = Index,
   Inc = ring_increment(NumPartitions),
   {A, B} = lists:split((IndexAsInt div Inc)+1, Nodes),
   B ++ A.

%% @doc Return increment between ring indexes given
%% the number of ring partitions.
ring_increment(NumPartitions) ->
   ?RINGTOP div NumPartitions.


