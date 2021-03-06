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
%% @doc The cache partition that temporarily stores the updates of 
%% local committed transactions to non-local keys. These updates
%% are visible to other local transactions and are removed when its 
%% transaction commits or aborts. When a transaction reads non-local keys,
%% it first goes to the local cache partition to see if there exists any
%% versions created by previous local committed transactions. If not, 
%% the read request is then forwarding to any replica of the corresponding
%% partition.
%%
%% The registered name of each node's cache is the name of it's erlang node. 

-module(cache_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-define(NUM_VERSIONS, 10).

%% API
-export([start_link/1,
        commit/4,
        local_commit/5,
        abort/2,
        if_prepared/2,
        num_specula_read/0,
        local_certify/3,
        read/4,
        read/2,
        read/3]).

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
        prepared_txs :: cache_id(),
        dep_dict :: dict(),
        specula_read :: boolean(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name},
             ?MODULE, [], []).

read(Name, Key, TxId, Node) ->
    gen_server:call(Name, {read, Key, TxId, Node}).

read(Key, TxId, Node) ->
    gen_server:call(node(), {read, Key, TxId, Node}).

read(Key, TxId) ->
    gen_server:call(node(), {read, Key, TxId}).

num_specula_read() ->
    gen_server:call(node(), {num_specula_read}).

if_prepared(TxId, Keys) ->
    gen_server:call(node(), {if_prepared, TxId, Keys}).

abort(TxId, Partition) -> 
    gen_server:cast(node(), {abort, TxId, Partition}).

commit(TxId, Partition, LOC, CommitTime) -> 
    gen_server:cast(node(), {commit, TxId, Partition, LOC, CommitTime}).

local_certify(TxId, Partition, WriteSet) ->
    gen_server:cast(node(), {local_certify, TxId, Partition, WriteSet, self()}).

local_commit(TxId, Partition, SpeculaCommitTs, LOC, FFC) ->
    gen_server:cast(node(), {local_commit, TxId, Partition, SpeculaCommitTs, LOC, FFC}).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    lager:info("Cache server inited"),
    PreparedTxs = tx_utilities:open_private_table(prepared_txs),
    SpeculaRead = antidote_config:get(specula_read),
    {ok, #state{specula_read = SpeculaRead, dep_dict=dict:new(),
                prepared_txs = PreparedTxs}}.

handle_call({num_specula_read}, _Sender, SD0) ->
    {reply, {0, 0}, SD0};

handle_call({get_pid}, _Sender, SD0) ->
        {reply, self(), SD0};

handle_call({clean_data}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    ets:delete_all_objects(PreparedTxs),
    {reply, ok, SD0};

handle_call({read, Key, TxId, _Node}, Sender, SD0=#state{specula_read=SpeculaRead}) ->
    handle_call({read, Key, TxId, _Node, SpeculaRead}, Sender, SD0);
handle_call({read, Key, TxId, {Partition, _}=Node, SpeculaRead}, Sender,
        SD0=#state{prepared_txs=PreparedTxs}) ->
    case SpeculaRead of
        false ->
            master_vnode:remote_read(Node, Key, TxId, Sender),
            {noreply, SD0};
        true ->
           %lager:warning("Cache specula read ~w of ~w from ~w", [Key, TxId, Sender]), 
            case local_cert_util:specula_read(TxId, {Partition, Key}, PreparedTxs, {TxId, Node, Sender}) of
                wait ->
                   %lager:warning("~w read wait!", [TxId]),
                    {noreply, SD0};
                not_ready->
                   %lager:warning("~w read blocked!", [TxId]),
                    {noreply, SD0};
                {specula, Value} ->
                   %lager:warning("~w read specula ~w", [TxId, Value]),
                    {reply, {ok, Value}, SD0};
                ready ->
                   %lager:warning("~w remote read!"),
                    master_vnode:remote_read(Node, Key, TxId, Sender),
                    {noreply, SD0}
            end
    end;

handle_call({read, Key, TxId}, _Sender,
        SD0=#state{prepared_txs=PreparedTxs, specula_read=false}) ->
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            case find_version(ValueList, MyClock) of
                {SpeculaTxId, Value} ->
                    ets:insert(dependency, {SpeculaTxId, TxId}),
                    %lager:info("Inserting anti_dep from ~w to ~w for ~p", [TxId, SpeculaTxId, Key]),
                    TxId = SpeculaTxId,
                    {reply, {ok, Value}, SD0};
                [] ->
                    {reply, {ok, []}, SD0}
            end
    end;

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs),
    {reply, Result, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

%% @doc: performs local certification. 
handle_cast({local_certify, TxId, Partition, WriteSet, Sender},
        SD0=#state{prepared_txs=PreparedTxs, dep_dict=DepDict}) ->
   %lager:warning("cache server local certify for [~w, ~w]", [TxId, Partition]),
    Result = local_cert_util:prepare_for_other_part(TxId, Partition, WriteSet, ignore, PreparedTxs, TxId#tx_id.snapshot_time, cache),
    case Result of
        {ok, PrepareTime} ->
            %lager:warning("~w: ~w certification check prepred with ~w", [Partition, TxId, PrepareTime]),
            gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}),
            {noreply, SD0};
        {wait, PendPrepDep, _PrepDep, PrepareTime} ->
            NewDepDict =
                case PendPrepDep of
                    0 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), DepDict;
                    _ -> dict:store(TxId, {PendPrepDep, PrepareTime, Sender}, DepDict)
                end,
            {noreply, SD0#state{dep_dict=NewDepDict}};
        {error, write_conflict} ->
            gen_server:cast(Sender, {aborted, TxId}),
            {noreply, SD0}
    end;

%% @doc: per-commit this transaction, by applying its updates in the local storage.
handle_cast({local_commit, TxId, Partition, SpeculaCommitTime, LOC, FFC}, State=#state{prepared_txs=PreparedTxs,
        dep_dict=DepDict}) ->
   %lager:warning("specula commit for [~w, ~w]", [TxId, Partition]),
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [{{TxId, Partition}, Keys}] ->
            DepDict1 = local_cert_util:local_commit(Keys, TxId, SpeculaCommitTime, ignore, PreparedTxs, DepDict, Partition, LOC, FFC, cache),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            lager:error("Prepared record of ~w has disappeared!", [TxId]),
            error
    end;

%% @doc: abort a transaction by removing its updates. Notify all 
%% transactions data-depend on it to abort. 
handle_cast({abort, TxId, Partitions}, 
	    SD0=#state{prepared_txs=PreparedTxs, dep_dict=DepDict}) ->
   %lager:warning("Abort ~w in cache", [TxId]),
    DepDict1 = lists:foldl(fun(Partition, D) ->
            case ets:lookup(PreparedTxs, {TxId, Partition}) of 
                [{{TxId, Partition}, KeySet}] -> 
                    ets:delete(PreparedTxs, {TxId, Partition}),
                    local_cert_util:clean_abort_prepared(PreparedTxs, KeySet, TxId, ignore, D, Partition, cache);
                _ -> D 
            end end, DepDict, Partitions),
    specula_utilities:deal_abort_deps(TxId),
    {noreply, SD0#state{dep_dict=DepDict1}};
    
%% @doc: commit this transaction, but all updates should also be cleaned. 
%% Notify all transactions that have read from it.
handle_cast({commit, TxId, Partitions, LOC, CommitTime}, 
	    SD0=#state{prepared_txs=PreparedTxs, dep_dict=DepDict}) ->
   %lager:warning("Commit ~w in cache", [TxId]),
    DepDict1 = lists:foldl(fun(Partition, D) ->
            case ets:lookup(PreparedTxs, {TxId, Partition}) of
                [{{TxId, Partition}, KeySet}] ->
                    ets:delete(PreparedTxs, {TxId, Partition}),
                    local_cert_util:update_store(KeySet, TxId, CommitTime, ignore, ignore, PreparedTxs,
                        D, Partition, cache);
                _ ->
                    D
            end end, DepDict, Partitions),
    specula_utilities:deal_commit_deps(TxId, LOC, CommitTime),
    {noreply, SD0#state{dep_dict=DepDict1}};


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

find_version([],  _SnapshotTime) ->
    [];
find_version([{TS, Value, TxId}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {TxId, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.

