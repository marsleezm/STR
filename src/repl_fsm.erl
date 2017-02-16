%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% @doc Repl_fsm is responsible for replicating prepare requests to a
%% partition's slave replicas, after the master has prepared it. 
%% Each node is equipped with a repl_fsm.
%%
-module(repl_fsm).

-behavior(gen_server).

-include("antidote.hrl").

%% API
-export([start_link/0,
         repl_prepare/4,
	     check_table/0,
         repl_abort/3,
         repl_commit/5,
         quorum_replicate/7,
         ack_pending_prep/2,
         chain_replicate/5]).

%% Callbacks
-export([init/1,
	    handle_call/3,
	    handle_cast/2,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-record(state, {
        replicas :: [atom()],
        except_replicas :: dict(),
        mode :: atom(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, repl_fsm},
             ?MODULE, [repl_fsm], []).

repl_prepare(Partition, PrepType, TxId, LogContent) ->
    gen_server:cast(repl_fsm, {repl_prepare, Partition, PrepType, TxId, LogContent}).

ack_pending_prep(TxId, Partition) ->
    gen_server:cast(repl_fsm, {ack_pending_prep, TxId, Partition}).

check_table() ->
    gen_server:call(repl_fsm, {check_table}).

quorum_replicate(Replicas, Type, TxId, Partition, WriteSet, TimeStamp, Sender) ->
    lists:foreach(fun(Replica) ->
            gen_server:cast({global, Replica}, {repl_prepare, 
                Type, TxId, Partition, WriteSet, TimeStamp, Sender})
            end, Replicas).

chain_replicate(_Replicas, _TxId, _WriteSet, _TimeStamp, _ToReply) ->
    ok.

%% Send a commit request to all replicas of updated partitions. The cache partition
%% is also included if this transaction has updated non-local keys.
repl_commit([], _, _, _, _) ->
    ok;
repl_commit(UpdatedParts, TxId, LOC, CommitTime, RepDict) ->
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache ->
                    cache_serv:commit(TxId, Partitions, LOC, CommitTime);
                {rep, S} ->
                    gen_server:cast({global, S}, {repl_commit, TxId, LOC, CommitTime, Partitions});
                S ->
                    gen_server:cast({global, S}, {repl_commit, TxId, LOC, CommitTime, Partitions})
            end end,  Replicas) end,
            NodeParts).

%% Send a abort request to all replicas of updated partitions. The cache partition
%% is also included if this transaction has updated non-local keys.
repl_abort([], _, _) ->
    ok;
repl_abort(UpdatedParts, TxId, RepDict) ->
   %lager:warning("Aborting to ~w", [UpdatedParts]),
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        %lager:info("repl abort of ~w for node ~w to replicas ~w for partitions ~w", [TxId, Node, Replicas, Partitions]),
        lists:foreach(fun(R) ->
            case R of
                cache -> 
                    cache_serv:abort(TxId, Partitions); 
                {rep, S} ->
                    gen_server:cast({global, S}, {repl_abort, TxId, Partitions});
                S ->
                    gen_server:cast({global, S}, {repl_abort, TxId, Partitions})
            end end,  Replicas) end,
            NodeParts).

%%%===================================================================
%%% Internal
%%%===================================================================


init([_Name]) ->
    hash_fun:build_rev_replicas(),
    [{_, Replicas}] = ets:lookup(meta_info, node()), 
   %lager:warning("Replicas are ~w", [Replicas]),
    NewDict = generate_except_replicas(Replicas),
    Lists = antidote_config:get(to_repl),
    [LocalRepList] = [LocalReps || {Node, LocalReps} <- Lists, Node == node()],
    LocalRepNames = [list_to_atom(atom_to_list(node())++"repl"++atom_to_list(L))  || L <- LocalRepList ],
    lager:info("NewDict is ~w, LocalRepNames is ~w", [NewDict, LocalRepNames]),
    {ok, #state{replicas=Replicas, mode=quorum, 
                except_replicas=NewDict}}.

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0};

handle_call({check_table}, _Sender, SD0) ->
    {reply, ok, SD0}.

%% @doc: Replicate LogContent of TxId to the slave replicas of Partition
handle_cast({repl_prepare, Partition, prepared, TxId, LogContent}, 
	    SD0=#state{replicas=Replicas, except_replicas=ExceptReplicas, mode=Mode}) ->
    {Sender, RepMode, WriteSet, PrepareTime} = LogContent,
    Mode = quorum,
    case RepMode of
        %% This is for non-specula version
        {remote, ignore} ->
            %lager:warning("Ignor Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Sender);
        {remote, SenderName} ->
            %% In speculation mode (with local commit), the prepare record is already applied to the node that sent this 
            %% prepare request, so there is no need to replicate the prepare there again. 
            case dict:find(SenderName, ExceptReplicas) of
                {ok, []} ->
                    ok;
                {ok, R} -> 
                    quorum_replicate(R, prepared, TxId, Partition, WriteSet, PrepareTime, Sender);
                error ->
                    quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Sender)
            end;
        _ ->
            %lager:warning("Prepared request for {~w, ~w}, Sending to ~w, ReplFactor is ~w", [TxId, Partition, Replicas, ReplFactor]),
            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Sender)
    end,
    {noreply, SD0};

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.


%%%===================================================================
%%% Internal
%%%===================================================================

build_node_parts(Parts) ->
    D = lists:foldl(fun({Partition, Node}, Acc) ->
                      dict:append(Node, Partition, Acc)
                   end,
                    dict:new(), Parts),
    dict:to_list(D).

generate_except_replicas(Replicas) ->
    lists:foldl(fun(Rep, D) ->
            Except = lists:delete(Rep, Replicas),
            NodeName = get_name(atom_to_list(Rep), 1),
            dict:store(list_to_atom(NodeName), Except, D)
        end, dict:new(), Replicas).

get_name(ReplName, N) ->
    case lists:sublist(ReplName, N, 4) of
        "repl" ->  lists:sublist(ReplName, 1, N-1);
         _ -> get_name(ReplName, N+1)
    end.
    
