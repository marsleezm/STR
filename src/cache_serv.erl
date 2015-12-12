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
%% TODO: should implement heart-beat for timestamp.
-module(cache_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NUM_VERSIONS, 10).
%% API
-export([start_link/1]).

-define(CLOCKSI_VNODE, clocksi_vnode).

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
-export([
        prepare_specula/4,
        commit_specula/3,
        abort_specula/2,
        if_prepared/2,
        read/4,
        read/2,
        read/3]).

%% Spawn

-record(state, {
        cache_log :: cache_id(),
        delay :: non_neg_integer(),
        do_specula :: boolean(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({global, Name},
             ?MODULE, [], []).

read(Name, Key, TxId, Node) ->
    gen_server:call({global, Name}, {read, Key, TxId, Node}).

read(Key, TxId, Node) ->
    gen_server:call({global, node()}, {read, Key, TxId, Node}).

read(Key, TxId) ->
    gen_server:call({global, node()}, {read, Key, TxId}).

if_prepared(TxId, Keys) ->
    gen_server:call({global, node()}, {if_prepared, TxId, Keys}).

prepare_specula(TxId, Partition, WriteSet, PrepareTime) ->
    gen_server:cast({global, node()}, {prepare_specula, TxId, Partition, WriteSet, PrepareTime}).

abort_specula(TxId, Partition) -> 
    gen_server:cast({global, node()}, {abort_specula, TxId, Partition}).

commit_specula(TxId, Partition, CommitTime) -> 
    gen_server:cast({global, node()}, {commit_specula, TxId, Partition, CommitTime}).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    lager:info("Cache server inited"),
    CacheLog = tx_utilities:open_private_table(cache_log),
    DoSpecula = antidote_config:get(do_specula),
    {ok, #state{do_specula = DoSpecula,
                cache_log = CacheLog}}.

handle_call({read, Key, TxId, Node}, Sender, 
	    SD0=#state{do_specula=false}) ->
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, no_specula),
    {noreply, SD0};

handle_call({read, Key, TxId, Node}, Sender, 
	    SD0=#state{cache_log=CacheLog, do_specula=true}) ->
    %lager:info("Read ~w of ~w", [Key, TxId]), 
    case ets:lookup(CacheLog, Key) of
        [] ->
            %lager:info("Relaying read to ~w", [Node]),
            ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, no_specula),
            %lager:info("Well, from clocksi_vnode"),
            %lager:info("Nothing!"),
            {noreply, SD0};
        [{Key, ValueList}] ->
            %lager:info("Value list is ~w", [ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            case find_version(ValueList, MyClock) of
                {SpeculaTxId, Value} ->
                    ets:insert(dependency, {SpeculaTxId, TxId}),         
                    %lager:info("Inserting anti_dep from ~w to ~w for ~p", [TxId, SpeculaTxId, Key]),
                    ets:insert(anti_dep, {TxId, SpeculaTxId}),        
                    %{reply, {{specula, SpeculaTxId}, Value}, SD0};
                    {reply, {ok, Value}, SD0};
                [] ->
                    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, no_specula),
                    %lager:info("Well, from clocksi_vnode"),
                    {noreply, SD0}
            end
    end;

handle_call({read, Key, TxId}, _Sender,
        SD0=#state{cache_log=CacheLog}) ->
    case ets:lookup(CacheLog, Key) of
        [] ->
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            case find_version(ValueList, MyClock) of
                {SpeculaTxId, Value} ->
                    ets:insert(dependency, {SpeculaTxId, TxId}),
                    %lager:info("Inserting anti_dep from ~w to ~w for ~p", [TxId, SpeculaTxId, Key]),
                    ets:insert(anti_dep, {TxId, SpeculaTxId}),
                    {reply, {ok, Value}, SD0};
                [] ->
                    {reply, {ok, []}, SD0}
            end
    end;


handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{cache_log=CacheLog}) ->
    Result = lists:all(fun(Key) ->
                    case ets:lookup(CacheLog, Key) of
                        [{Key, [{_, _, TxId}|_]}] -> true;
                        _ -> false
                    end end, Keys),
    {reply, Result, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepare_specula, TxId, Partition, WriteSet, TimeStamp}, 
	    SD0=#state{cache_log=CacheLog}) ->
    KeySet = lists:foldl(fun({Key, Value}, KS) ->
                    case ets:lookup(CacheLog, Key) of
                        [] ->
                            %% Putting TxId in the record to mark the transaction as speculative 
                            %% and for dependency tracking that will happen later
                            true = ets:insert(CacheLog, {Key, [{TimeStamp, Value, TxId}]}),
                            [Key|KS];
                        [{Key, ValueList}] ->
                            {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                            true = ets:insert(CacheLog, {Key, [{TimeStamp, Value, TxId}|RemainList]}),
                            [Key|KS]
                    end end, [], WriteSet),
    ets:insert(CacheLog, {{TxId, Partition}, KeySet}),
    {noreply, SD0};

%% Where shall I put the speculative version?
%% In ets, faster for read.
handle_cast({abort_specula, TxId, Partition}, 
	    SD0=#state{cache_log=CacheLog}) ->
    [{{TxId, Partition}, KeySet}] = ets:lookup(CacheLog, {TxId, Partition}),
    delete_keys(CacheLog, KeySet, TxId),
    specula_utilities:deal_abort_deps(TxId),
    {noreply, SD0};
    
handle_cast({commit_specula, TxId, Partition, CommitTime}, 
	    SD0=#state{cache_log=CacheLog}) ->
    [{{TxId, Partition}, KeySet}] = ets:lookup(CacheLog, {TxId, Partition}),
    delete_keys(CacheLog, KeySet, TxId),
    specula_utilities:deal_commit_deps(TxId, CommitTime),
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

find_version([],  _SnapshotTime) ->
    [];
find_version([{TS, Value, TxId}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {TxId, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.


delete_version([{_, _, TxId}|Rest], TxId) -> 
    Rest;
delete_version([{TS, V, Tx}|Rest], TxId) -> 
    [{TS, V, Tx}|delete_version(Rest, TxId)].

delete_keys(Table, KeySet, TxId) ->
    lists:foreach(fun(Key) ->
                    case ets:lookup(Table, Key) of
                        [] -> %% TODO: this can not happen 
                            ok;
                        [{Key, ValueList}] ->
                            %lager:info("Delete version ~w, key is ~w, list is ~p", [TxId, Key, ValueList]),
                            NewValueList = delete_version(ValueList, TxId), 
                            %lager:info("after deletion list is ~p", [NewValueList]),
                            ets:insert(Table, {Key, NewValueList})
                    end 
                  end, KeySet).
