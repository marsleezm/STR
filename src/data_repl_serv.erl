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
%% @doc This gen_server represents a slave replica of a partition. Each 
%% slave partition is synchronously replicated by its master replica 
%% to store prepare requests. It maintains prepare requests until 
%% corresponding transactions is committed or aborted.  
%% A slave replica can be directly read bypassing its master replicas,
%% therefore it also has to track the 'last_reader' of each key and 
%% participates in proposing the prepare timestamps of transactions.
%%

-module(data_repl_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NUM_VERSIONS, 40).
-define(READ_TIMEOUT, 15000).
%% API
-export([start_link/2,
        relay_read/4,
        get_table/1,
        get_size/1,
        read_all/1,
	    check_key/2,
        clean_data/2,
	    check_table/1,
        verify_table/2,
        debug_read/3,
        local_certify/4,
        local_commit/6,
        if_prepared/3,
        if_bulk_prepared/3,
        num_specula_read/1,
        single_read/2,
        direct_read/4, 
        read/4]).

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
        inmemory_store :: cache_id(),
        prepared_txs :: cache_id(),
        committed_txs :: cache_id(),
        dep_dict :: dict(),
        table_size = 0,
        set_size :: non_neg_integer(),
        current_dict :: dict(),
        backup_dict :: dict(),
        specula_read :: boolean(),
        name :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name, Parts) ->
    gen_server:start_link({global, Name},
             ?MODULE, [Name, Parts], []).

read(Name, Key, TxId, Part) ->
    gen_server:call({global, Name}, {read, Key, TxId, Part}, ?READ_TIMEOUT).

direct_read(Name, Key, TxId, Part) ->
    gen_server:call(Name, {read, Key, TxId, Part}, ?READ_TIMEOUT).

get_table(Name) ->
    gen_server:call({global, Name}, {get_table}, ?READ_TIMEOUT).

get_size(Name) ->
    gen_server:call({global, Name}, {get_size}, ?READ_TIMEOUT).

single_read(Name, Key) ->
    TxId = tx_utilities:create_tx_id(0),
    gen_server:call({global, Name}, {read, Key, TxId}, ?READ_TIMEOUT).

verify_table(Name, List) ->
    gen_server:call({global, Name}, {verify_table, List}, infinity).

check_table(Name) ->
    gen_server:call({global, Name}, {check_table}).

check_key(Name, Key) ->
    gen_server:call({global, Name}, {check_key, Key}).

num_specula_read(Node) ->
    gen_server:call({global, Node}, {num_specula_read}).

debug_read(Name, Key, TxId) ->
    gen_server:call({global, Name}, {debug_read, Key, TxId}).

if_prepared(Name, TxId, Keys) ->
    gen_server:call({global, Name}, {if_prepared, TxId, Keys}).

if_bulk_prepared(Name, TxId, Partition) ->
    gen_server:call({global, Name}, {if_bulk_prepared, TxId, Partition}).

local_certify(Name, TxId, Partition, WriteSet) ->
    gen_server:cast({global, Name}, {local_certify, TxId, Partition, WriteSet, self()}).

read_all(Name) ->
    gen_server:cast({global, Name}, {get_size}).

local_commit(Name, TxId, Partition, SpeculaCommitTs, LOC, FFC) ->
    gen_server:cast({global, Name}, {local_commit, TxId, Partition, SpeculaCommitTs, LOC, FFC}).

relay_read(Name, Key, TxId, Reader) ->
    gen_server:cast({global, Name}, {relay_read, Key, TxId, Reader}).

clean_data(Name, Sender) ->
    gen_server:cast({global, Name}, {clean_data, Sender}).

%%%===================================================================
%%% Internal
%%%===================================================================

init([Name, _Parts]) ->
    InMemoryStore = tx_utilities:open_public_table(repl_log),
    PreparedTxs = tx_utilities:open_private_table(prepared_txs),
    CommittedTxs = tx_utilities:open_private_table(committed_txs),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    Concurrent = antidote_config:get(concurrent),
    SpeculaLength = antidote_config:get(specula_length),
    SetSize = min(max(TotalReplFactor*10*Concurrent*max(SpeculaLength,6), 5000), 40000),
    lager:info("Data repl inited with name ~w, repl factor is ~w, set size is ~w", [Name, TotalReplFactor, SetSize]),
    SpeculaRead = antidote_config:get(specula_read),
    {ok, #state{name=Name, set_size=SetSize, specula_read=SpeculaRead,
                prepared_txs = PreparedTxs, current_dict = dict:new(), committed_txs=CommittedTxs, dep_dict=dict:new(), 
                backup_dict = dict:new(), inmemory_store = InMemoryStore}}.

handle_call({get_table}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    {reply, InMemoryStore, SD0};

handle_call({get_pid}, _Sender, SD0) ->
        {reply, self(), SD0};

%% @doc: return the size of all tables. The first one is the table of preapred records (containing
%% LastReader for each key), the second is the size of the timestamp of last committed transaction,
%% and the third is the size of all committed records. The last one is the total size of all.
handle_call({get_size}, _Sender, SD0=#state{
          prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, committed_txs=CommittedTxs}) ->
  TableSize = ets:info(InMemoryStore, memory) * erlang:system_info(wordsize),
  PrepareSize = ets:info(PreparedTxs, memory) * erlang:system_info(wordsize),
  CommittedSize = ets:info(CommittedTxs, memory) * erlang:system_info(wordsize),
  {reply, {PrepareSize, CommittedSize, TableSize, TableSize+PrepareSize+CommittedSize}, SD0};

handle_call({num_specula_read}, _Sender, SD0) ->
    {reply, {0, 0}, SD0};

handle_call({check_table}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    lager:info("Log info: ~w", [ets:tab2list(PreparedTxs)]),
    {reply, ok, SD0};

handle_call({check_key, Key}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    Result = ets:lookup(InMemoryStore, Key),
    {reply, Result, SD0};

handle_call({debug_read, Key, TxId}, Sender, 
	    SD0=#state{inmemory_store=InMemoryStore}) ->
    lager:info("Got debug read for ~w from ~p", [Key, TxId]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            lager:info("Debug reading ~w, there is nothing", [Key]),
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            lager:info("Debug reading ~w, Value list is ~w", [Key, ValueList]),
            local_cert_util:read_value(Key, TxId, Sender, InMemoryStore),
            {noreply, SD0}
    end;

%% @doc: read a key. Depending on configuration, speculative read may or may not be allowed.
handle_call({read, Key, TxId, _Node}, Sender, SD0=#state{specula_read=SpeculaRead}) ->
    handle_call({read, Key, TxId, _Node, SpeculaRead}, Sender, SD0);
handle_call({read, Key, TxId, _Node, SpeculaRead}, Sender,
        SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs}) ->
    case SpeculaRead of
        false ->
           %lager:warning("Specula rea on data repl and false!!??"),
            case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, {TxId, Sender}) of
                not_ready-> {noreply, SD0};
                ready ->
                    %lager:warning("Read finished!"),
                    local_cert_util:read_value(Key, TxId, Sender, InMemoryStore),
                    {noreply, SD0}
            end;
        true ->
            %lager:warning("Specula read!!"),
            case local_cert_util:specula_read(TxId, Key, PreparedTxs, {TxId, Sender}) of
                wait->
                    %lager:warning("Read wait!"),
                    {noreply, SD0};
                not_ready->
                    {noreply, SD0};
                {specula, Value} ->
                    {reply, {ok, Value}, SD0};
                ready ->
                    %lager:warning("Read finished!"),
                    local_cert_util:read_value(Key, TxId, Sender, InMemoryStore),
                    {noreply, SD0}
            end
    end;

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs),
    {reply, Result, SD0};

handle_call({if_bulk_prepared, TxId, Partition}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs}) ->
    lager:info("checking if bulk_prepared for ~w ~w", [TxId, Partition]),
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [{{TxId, Partition}, _}] ->
            lager:info("It's inserted"),
            {reply, true, SD0};
        Record ->
            lager:info("~w: something else", [Record]),
            {reply, false, SD0}
    end;

handle_call({verify_table, List}, _Sender, SD0=#state{name=Name, inmemory_store=InMemoryStore}) ->
   lager:info("Start verifying on ~w", [Name]),
   lists:foreach(fun({K, V}=Elem) -> 
                    case ets:lookup(InMemoryStore, K) of [Elem] -> ok;
                        Other -> 
                            lager:error("Doesn't match! Origin is [{~p, ~p}], Rep is ~p", [K, V, Other])
                    end end, List),
   {reply, ok, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({relay_read, Key, TxId, Reader}, 
	    SD0=#state{inmemory_store=InMemoryStore}) ->
      %lager:warning("~w, ~p data repl read", [TxId, Key]),
    local_cert_util:read_value(Key, TxId, Reader, InMemoryStore),
    {noreply, SD0};

%% @doc: read all keys of this table. Essentially, update the last reader of all keys in the table.
handle_cast({read_all}, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    Now = tx_utilities:now_microsec(),
    lists:foreach(fun({Key, _}) ->
            ets:insert(PreparedTxs, {Key, Now})
            end, ets:tab2list(InMemoryStore)),
    {noreply, SD0};

%% @doc: local certification. Check if there is any conflicting concurrent transactions.
%% Transactions may either be prepared, aborted or be blocked because there are previous prepared
%% transactions.
handle_cast({local_certify, TxId, Partition, WriteSet, Sender}, 
	    SD0=#state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs, dep_dict=DepDict}) ->
   %lager:warning("local certify for [~w, ~w]", [TxId, Partition]),
    Result = local_cert_util:prepare_for_other_part(TxId, Partition, WriteSet, CommittedTxs, PreparedTxs, TxId#tx_id.snapshot_time+1, slave),
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

%% @doc: pre-commit this tranaction by convering the state of its prepared records to local-commit state and
%% adding local-commit timestamp.
handle_cast({local_commit, TxId, Partition, SpeculaCommitTime, LOC, FFC}, State=#state{prepared_txs=PreparedTxs,
        inmemory_store=InMemoryStore, dep_dict=DepDict}) ->
   %lager:warning("Specula commit for ~w ", [Partition]),
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [{{TxId, Partition}, Keys}] ->
            DepDict1 = local_cert_util:local_commit(Keys, TxId, SpeculaCommitTime, InMemoryStore, PreparedTxs, DepDict, Partition, LOC, FFC, slave),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            lager:error("Prepared record of ~w, ~w has disappeared!", [TxId, Partition]),
            error
    end;

handle_cast({clean_data, Sender}, SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs,
            committed_txs=CommittedTxs}) ->
    ets:delete_all_objects(PreparedTxs),
    ets:delete_all_objects(InMemoryStore),
    ets:delete_all_objects(CommittedTxs),
    Sender ! cleaned,
    {noreply, SD0#state{prepared_txs = PreparedTxs, current_dict = dict:new(), backup_dict = dict:new(), 
                table_size=0, inmemory_store = InMemoryStore, committed_txs=CommittedTxs}};

handle_cast({repl_prepare, Type, TxId, Part, WriteSet, TimeStamp, Sender}, 
	    SD0=#state{prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, current_dict=CurrentDict, backup_dict=BackupDict}) ->
    case Type of
        prepared ->
            case dict:find(TxId, CurrentDict) of
                {ok, finished} ->
                    {noreply, SD0};
                error ->
                    case dict:find(TxId, BackupDict) of
                        {ok, finished} ->
                            {noreply, SD0};
                        error ->
                            local_cert_util:insert_prepare(PreparedTxs, TxId, Part, WriteSet, TimeStamp, Sender),
                            {noreply, SD0}
                    end
            end;
        single_commit ->
            AppendFun = fun({Key, Value}) ->
                case ets:lookup(InMemoryStore, Key) of
                    [] ->
                         %lager:warning("Data repl inserting ~p, ~p of ~w to table", [Key, Value, TimeStamp]),
                        true = ets:insert(InMemoryStore, {Key, [{TimeStamp, Value}]});
                    [{Key, ValueList}] ->
                        {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                        true = ets:insert(InMemoryStore, {Key, [{TimeStamp, Value}|RemainList]})
                end end,
            lists:foreach(AppendFun, WriteSet),
            gen_server:cast({global, Sender}, {ack, Part, TxId}), 
            {noreply, SD0}
    end;

handle_cast({repl_commit, TxId, LOC, CommitTime, Partitions}, 
	    SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs, specula_read=SpeculaRead, committed_txs=CommittedTxs,
        dep_dict=DepDict}) ->
  %lager:warning("Repl commit for ~w, ~w", [TxId, Partitions]),
   DepDict1 = lists:foldl(fun(Partition, D) ->
                    [{{TxId, Partition}, KeySet}] = ets:lookup(PreparedTxs, {TxId, Partition}), 
                    ets:delete(PreparedTxs, {TxId, Partition}),
                    local_cert_util:update_store(KeySet, TxId, CommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                        D, Partition, slave)
                    %end
        end, DepDict, Partitions),
    case SpeculaRead of
        true -> specula_utilities:deal_commit_deps(TxId, LOC, CommitTime); 
        _ -> ok
    end,
    {noreply, SD0#state{dep_dict=DepDict1}};

handle_cast({repl_abort, TxId, Partitions}, SD0=#state{prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, 
    specula_read=SpeculaRead, current_dict=CurrentDict, dep_dict=DepDict, set_size=SetSize, table_size=TableSize}) ->
   %lager:warning("repl abort for ~w ~w", [TxId, Partitions]),
    {IfMissed, DepDict1} = lists:foldl(fun(Partition, {IfMiss, DepD}) ->
               case ets:lookup(PreparedTxs, {TxId, Partition}) of
                    [{{TxId, Partition}, KeySet}] ->
                        %lager:warning("Found for ~w, ~w, KeySet is ~w", [TxId, Partition, KeySet]),
                        ets:delete(PreparedTxs, {TxId, Partition}),
                        DepD1= local_cert_util:clean_abort_prepared(PreparedTxs, KeySet, TxId, InMemoryStore, DepD, Partition, slave),
                        {IfMiss, DepD1};
                    [] ->
                       %lager:warning("Repl abort arrived early! ~w", [TxId]),
                        {true, DepD}
                end
        end, {false, DepDict}, Partitions),
    case SpeculaRead of
        true -> specula_utilities:deal_abort_deps(TxId);
        _ -> ok
    end,
    case IfMissed of
        true ->
            CurrentDict1 = dict:store(TxId, finished, CurrentDict),
            case TableSize > SetSize of
                true ->
                    %lager:warning("Current set is too large!"),
                    {noreply, SD0#state{current_dict=dict:new(), backup_dict=CurrentDict1, table_size=0, dep_dict=DepDict1}};
                false ->
                    {noreply, SD0#state{current_dict=CurrentDict1, dep_dict=DepDict1, table_size=TableSize+1}}
            end;
        false ->
            {noreply, SD0#state{dep_dict=DepDict1}}
    end;

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

