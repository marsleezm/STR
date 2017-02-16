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
%% @doc The master replica of a partition. Each master partition recieves
%% prepare request for transactions. After successfully preparing a 
%% transaction, it replies the prepare timestamp to the transaction 
%% coordinator and also replicates the prepare request to its slave replicas. 
%%
-module(master_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_VERSION, 40).
-define(SPECULA_THRESHOLD, 0).

%% API
-export([start_vnode/1,
	    internal_read/3,
        debug_read/3,
	    relay_read/5,
        remote_read/4,
        get_size/1,
        get_table/1,
        clean_data/2,
        local_commit/5,
        set_debug/2,
        do_reply/2,
        debug_prepare/4,
        prepare/3,
        prepare/4,
        commit/4,
        append_value/5,
        append_values/4,
        abort/2,
        read_all/1,
        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1]).

%% Callbacks.
-export([handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).


-ignore_xref([start_vnode/1]).

-record(state, {partition :: non_neg_integer(),
                prepared_txs :: cache_id(),
                committed_txs :: cache_id(),
                if_certify :: boolean(),
                if_replicate :: boolean(),
                if_specula :: boolean(),
                inmemory_store :: cache_id(),
                dep_dict :: dict(),
                debug = false :: boolean()
                }).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

set_debug(Node, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                           {set_debug, Debug},
                           ?CLOCKSI_MASTER, infinity).

do_reply(Node, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                           {do_reply, TxId},
                           ?CLOCKSI_MASTER, infinity).

get_table(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                   {get_table},
                                   ?CLOCKSI_MASTER, infinity).

internal_read(Node, Key, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                                   {internal_read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

debug_read(Node, Key, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                                   {debug_read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

get_size(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                   {get_size},
                                   ?CLOCKSI_MASTER, infinity).

read_all(Node) ->
    riak_core_vnode_master:command(Node,
                                   {read_all}, self(),
                                   ?CLOCKSI_MASTER).

clean_data(Node, From) ->
    riak_core_vnode_master:command(Node,
                                   {clean_data, From}, self(),
                                   ?CLOCKSI_MASTER).

relay_read(Node, Key, TxId, Reader, From) ->
    riak_core_vnode_master:command(Node,
                                   {relay_read, Key, TxId, Reader, From}, self(),
                                   ?CLOCKSI_MASTER).

remote_read(Node, Key, TxId, Reader) ->
    riak_core_vnode_master:command(Node,
                                   {remote_read, Key, TxId, Reader}, self(),
                                   ?CLOCKSI_MASTER).

prepare(Updates, TxId, Type) ->
    ProposeTime = TxId#tx_id.snapshot_time+1,
    lists:foldl(fun({Node, WriteSet}, {Partitions, NumPartitions}) ->
        riak_core_vnode_master:command(Node,
                           {prepare, TxId, WriteSet, Type, ProposeTime},
                           self(),
                           ?CLOCKSI_MASTER),
        {[Node|Partitions], NumPartitions+1}
    end, {[], 0}, Updates).

prepare(Updates, CollectedTS, TxId, Type) ->
    ProposeTS = max(TxId#tx_id.snapshot_time+1, CollectedTS),
    lists:foreach(fun({Node, WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type, ProposeTS},
                               self(),
						       ?CLOCKSI_MASTER)
		end, Updates).

local_commit(Partitions, TxId, SpeculaCommitTime, LOC, FFC) ->
    lists:foreach(fun(Partition) ->
			riak_core_vnode_master:command(Partition,
						       {local_commit, TxId, SpeculaCommitTime, LOC, FFC},
                               self(), ?CLOCKSI_MASTER)
            end, Partitions).

debug_prepare(Updates, TxId, Type, Sender) ->
    ProposeTime = TxId#tx_id.snapshot_time+1,
    lists:foreach(fun({Node, WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type, ProposeTime},
                               Sender,
						       ?CLOCKSI_MASTER)
		end, Updates).

append_value(Node, Key, Value, CommitTime, ToReply) ->
    riak_core_vnode_master:command(Node,
                                   {append_value, Key, Value, CommitTime},
                                   ToReply,
                                   ?CLOCKSI_MASTER).

append_values(Node, KeyValues, CommitTime, _ToReply) ->
    riak_core_vnode_master:sync_command(Node,
                                   {append_values, KeyValues, CommitTime},
                                   ?CLOCKSI_MASTER, infinity).

commit(UpdatedParts, TxId, CommitTime, LOC) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime, LOC},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, UpdatedParts).

abort(UpdatedParts, TxId) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, UpdatedParts).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    lager:info("Initing partition ~w", [Partition]),
    PreparedTxs = tx_utilities:open_public_table(prepared),
    CommittedTxs = tx_utilities:open_public_table(committed),
    InMemoryStore = tx_utilities:open_public_table(inmemory_store),
    DepDict = dict:new(),
    DepDict1 = dict:store(commit_diff, {0,0}, DepDict),
    IfReplicate = antidote_config:get(do_repl), 
    IfSpecula = antidote_config:get(do_specula), 
    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,
    {ok, #state{partition=Partition,
                committed_txs=CommittedTxs,
                prepared_txs=PreparedTxs,
                if_replicate = IfReplicate,
                if_specula = IfSpecula,
                inmemory_store=InMemoryStore,
                dep_dict = DepDict1
                }}.

handle_command({set_debug, Debug},_Sender,SD0=#state{partition=_Partition}) ->
    {reply, ok, SD0#state{debug=Debug}};

handle_command({get_table}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    {reply, InMemoryStore, SD0};

handle_command({verify_table, Repl},_Sender,SD0=#state{inmemory_store=InMemoryStore}) ->
    R = helper:handle_verify_table(Repl, InMemoryStore),
    {reply, R, SD0};

handle_command({clean_data, Sender}, _Sender, SD0=#state{inmemory_store=InMemoryStore,
        prepared_txs=PreparedTxs, committed_txs=CommittedTxs, partition=Partition}) ->
    ets:delete_all_objects(InMemoryStore),
    ets:delete_all_objects(PreparedTxs),
    ets:delete_all_objects(CommittedTxs),
    DepDict = dict:new(),
    DepDict1 = dict:store(commit_diff, {0,0}, DepDict),
    Sender ! cleaned,
    {noreply, SD0#state{partition=Partition,
                dep_dict = DepDict1}};

handle_command({relay_read_stat},_Sender,SD0) -> 
    {reply, 0, SD0};

handle_command({num_specula_read},_Sender,SD0) ->
    {reply, 0, SD0};

handle_command({check_key_record, Key, Type},_Sender,SD0=#state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs}) ->
    R = helper:handle_check_key_record(Key, Type, PreparedTxs, CommittedTxs),
    {reply, R, SD0};

handle_command({check_top_aborted, _},_Sender,SD0=#state{dep_dict=DepDict}) -> 
    R = helper:handle_check_top_aborted(DepDict),
    {reply, R, SD0};

handle_command({do_reply, TxId}, _Sender, SD0=#state{prepared_txs=PreparedTxs, if_replicate=IfReplicate}) ->
    [{{pending, TxId}, Result}] = ets:lookup(PreparedTxs, {pending, TxId}),
    ets:delete(PreparedTxs, {pending, TxId}),
    case IfReplicate of
        true ->
            %lager:warning("Start replicate ~w", [TxId]),
            {Sender, _RepMode, _WriteSet, PrepareTime} = Result,
            gen_server:cast(Sender, {prepared, TxId, PrepareTime}),
            {reply, ok, SD0};
        false ->
            %lager:warning("Start replying ~w", [TxId]),
            {From, Reply} = Result,
            gen_server:cast(From, Reply),
            {reply, ok, SD0}
    end;

handle_command({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    %lager:warning("Checking if prepared of ~w for ~w", [TxId, Keys]),
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs), 
    {reply, Result, SD0};

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = helper:handle_check_tables_ready(Partition), 
    {reply, Result, SD0};

handle_command({print_stat},_Sender,SD0=#state{partition=_Partition}) ->
    {reply, ok, SD0};
    
handle_command({check_prepared_empty},_Sender,SD0=#state{prepared_txs=PreparedTxs}) ->
    R = helper:handle_command_check_prepared_empty(PreparedTxs),
    {reply, R, SD0};

handle_command({check_servers_ready},_Sender,SD0) ->
    {reply, true, SD0};

handle_command({debug_read, Key, TxId}, Sender, SD0=#state{
            inmemory_store=InMemoryStore, partition=_Partition}) ->
    local_cert_util:read_value(Key, TxId, Sender, InMemoryStore),
    {noreply, SD0};

%% Internal read is not specula
handle_command({internal_read, Key, TxId}, Sender, SD0=#state{%num_blocked=NumBlocked, 
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, partition=_Partition}) ->
    {_, _, RealSender} = Sender,
    case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, RealSender) of
        not_ready->
            %lager:info("Not ready for ~w", [Key]),
            {noreply, SD0};
        ready ->
            local_cert_util:read_value(Key, TxId, RealSender, InMemoryStore),
            %lager:info("Got value for ~w, ~w", [Key, Result]),
            {noreply, SD0}
    end;

handle_command({get_size}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, committed_txs=CommittedTxs}) ->
    TableSize = ets:info(InMemoryStore, memory) * erlang:system_info(wordsize),
    PrepareSize = ets:info(PreparedTxs, memory) * erlang:system_info(wordsize),
    CommittedSize = ets:info(CommittedTxs, memory) * erlang:system_info(wordsize),
    {reply, {PrepareSize, CommittedSize, TableSize, TableSize+PrepareSize+CommittedSize}, SD0};

handle_command({read_all}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    Now = tx_utilities:now_microsec(),
    lists:foreach(fun({Key, _}) ->
            ets:insert(PreparedTxs, {Key, Now})
            end, ets:tab2list(InMemoryStore)),
    {noreply, SD0};

%% @doc: Server the read to Key from transaction TxId. Reader is the process that
%% sent the read request.
handle_command({relay_read, Key, TxId, Reader, SpeculaRead}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    %lager:warning("Relaying read for ~p ~w", [Key, TxId]),
    case SpeculaRead of
        false ->
            case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, {TxId, Reader}) of
                not_ready->
                    %lager:warning("Read for ~p of ~w blocked!", [Key, TxId]),
                    {noreply, SD0};
                ready ->
                    local_cert_util:read_value(Key, TxId, Reader, InMemoryStore),
                    {noreply, SD0}
            end;
        true ->
            case local_cert_util:specula_read(TxId, Key, PreparedTxs, {TxId, Reader}) of
                wait ->
                    {noreply, SD0};
                not_ready->
                    {noreply, SD0};
                {specula, Value} ->
                    gen_server:reply(Reader, {ok, Value}), 
                    {noreply, SD0};
                ready ->
                    local_cert_util:read_value(Key, TxId, Reader, InMemoryStore),
                    {noreply, SD0}
            end
    end;

%% @doc: Server remote read requests, i.e. read requests sent by transactions initialized from
%% remote nodes.
handle_command({remote_read, Key, TxId, Reader}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
   %lager:warning("Remote read for ~p ~w", [Key, TxId]),
    case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, {TxId, {remote, Reader}}) of
        not_ready->
           %lager:warning("Read for ~p of ~w blocked!", [Key, TxId]),
            {noreply, SD0};
        ready ->
            local_cert_util:remote_read_value(Key, TxId, Reader, InMemoryStore),
            {noreply, SD0}
    end;

%% RepMode:
%%  local: local certification, needs to send prepare to all slaves
%%  remote, AvoidNode: remote cert, do not need to send prepare to AvoidNode
handle_command({prepare, TxId, WriteSet, RepMode, ProposedTs}, RawSender,
               State = #state{partition=Partition,
                              if_specula=IfSpecula,
                              committed_txs=CommittedTxs,
                              prepared_txs=PreparedTxs,
                              dep_dict=DepDict,
                              debug=Debug
                              }) ->
    Sender = case RawSender of {debug, RS} -> RS; _ -> RawSender end,
    %lager:warning("~w received prepare for ~w", [Partition, TxId]),
    Result = local_cert_util:prepare_for_master_part(TxId, WriteSet, CommittedTxs, PreparedTxs, ProposedTs),
    case Result of
        {ok, PrepareTime} ->
            %lager:warning("~w: ~w certification check prepred with ~w, RepMode is ~w", [Partition, TxId, PrepareTime, RepMode]),
            case Debug of
                false ->
                    PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                    repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                    gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}),
                    {noreply, State};
                true ->
                    PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                    repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                    ets:insert(PreparedTxs, {{pending, TxId}, PendingRecord}),
                    {noreply, State}
            end;
        {wait, PendPrepDep, PrepDep, PrepareTime} ->
            %lager:warning("Wait prepared for ~w at ~w", [TxId, Partition]),
            case IfSpecula of
                true ->
                    NewDepDict 
                        = case (PendPrepDep == 0) and (RepMode == local) of
                            true ->  
                                gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime}),
                                RepMsg = {Sender, pending_prepared, WriteSet, PrepareTime},
                                repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                                dict:store(TxId, {0, PrepDep, PrepareTime, Sender, RepMode}, DepDict);
                            _ ->
                                dict:store(TxId, {PendPrepDep, PrepDep, PrepareTime, Sender, RepMode, WriteSet}, DepDict)
                          end, 
                    {noreply, State#state{dep_dict=NewDepDict}};
                false ->
                    PrepDep = 0,
                    NewDepDict = dict:store(TxId, {PendPrepDep, 0, PrepareTime, Sender, RepMode, WriteSet}, DepDict),
                    {noreply, State#state{dep_dict=NewDepDict}}
            end;
        {error, write_conflict} ->
            %lager:warning("~w: ~w cerfify abort", [Partition, TxId]),
            case Debug of
                false ->
                    case RepMode of 
                        local ->
                            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
                            {noreply, State};
                        _ ->
                            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
                            {noreply, State}
                    end;
                true ->
                    ets:insert(PreparedTxs, {{pending, TxId}, {Sender, {aborted, TxId, RepMode}}}),
                    {noreply, State}
            end 
    end;

%% @doc: Local commit a transaction.
handle_command({local_commit, TxId, SpeculaCommitTime, LOC, FFC}, _Sender, State=#state{prepared_txs=PreparedTxs,
        inmemory_store=InMemoryStore, dep_dict=DepDict, partition=Partition}) ->
    %lager:warning("Got specula commit for ~w", [TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            DepDict1 = local_cert_util:local_commit(Keys, TxId, SpeculaCommitTime, InMemoryStore, PreparedTxs, DepDict, Partition, LOC, FFC, master),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            lager:error("Prepared record of ~w has disappeared!", [TxId]),
            {noreply, State}
    end;

handle_command({append_value, Key, Value, CommitTime}, Sender,
               State = #state{committed_txs=CommittedTxs,
                              inmemory_store=InMemoryStore
                              }) ->
    ets:insert(CommittedTxs, {Key, CommitTime}),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]});
        [{Key, ValueList}] ->
            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}|RemainList]})
    end,
    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
    {noreply, State};

handle_command({append_values, KeyValues, CommitTime}, _Sender,
               State = #state{committed_txs=CommittedTxs,
                              inmemory_store=InMemoryStore
                              }) ->
    lists:foreach(fun({Key, Value}) ->
            ets:insert(CommittedTxs, {Key, CommitTime}),
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]})
            end, KeyValues),
    {reply, {ok, committed}, State};

handle_command({commit, TxId, LOC, TxCommitTime}, _Sender,
               #state{partition=Partition,
                      committed_txs=CommittedTxs,
                      prepared_txs=PreparedTxs,
                      inmemory_store=InMemoryStore,
                      dep_dict = DepDict,
                      if_specula=IfSpecula
                      } = State) ->
    %lager:warning("~w: Got commit req for ~w with ~w", [Partition, TxId, TxCommitTime]),
    Result = commit(TxId, LOC, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, Partition, IfSpecula),
    case Result of
        {ok, committed, DepDict1} ->
            {noreply, State#state{dep_dict=DepDict1}};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId}, _Sender,
               State = #state{partition=Partition, prepared_txs=PreparedTxs, inmemory_store=InMemoryStore,
                dep_dict=DepDict, if_specula=IfSpecula}) ->
    %lager:warning("~w: Aborting ~w", [Partition, TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            case IfSpecula of
                true -> specula_utilities:deal_abort_deps(TxId);
                false -> ok
            end,
            %lager:warning("Found key set"),
            true = ets:delete(PreparedTxs, TxId),
            DepDict1 = local_cert_util:clean_abort_prepared(PreparedTxs, Keys, TxId, InMemoryStore, DepDict, Partition, master),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            %lager:error("No key set at all for ~w", [TxId]),
            {noreply, State}
    end;

handle_command({start_read_servers}, _Sender, State) ->
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
    term_to_binary({StatName,Val}).

is_empty(State) ->
    {true,State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs, 
                            inmemory_store=InMemoryStore} = _State) ->
    ets:delete(PreparedTxs),
    ets:delete(CommittedTxs),
    ets:delete(InMemoryStore),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
commit(TxId, LOC, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, Partition, IfSpecula)->
    %lager:warning("Before commit ~w", [TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            case IfSpecula of
                true -> specula_utilities:deal_commit_deps(TxId, LOC, TxCommitTime);
                false -> ok
            end,
            DepDict1 = local_cert_util:update_store(Keys, TxId, TxCommitTime, InMemoryStore, CommittedTxs, 
                PreparedTxs, DepDict, Partition, master),
            true = ets:delete(PreparedTxs, TxId),
            {ok, committed, DepDict1};
        [] ->
            %lager:error("Prepared record of ~w has disappeared!", [TxId]),
            error
    end.
