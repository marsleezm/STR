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
%% @doc The coordinator for a given Clock SI general tx_id.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(specula_general_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").
-include("speculation.hrl").

-define(SPECULA_TIMEOUT, 0).
-define(DUMB_TIMEOUT, 50).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(LOG_UTIL, mock_partition_fsm).
-define(PARTITION_VNODE, mock_partition_fsm).
-define(PARTITION_DOWNSTREAM, mock_partition_fsm).
-else.
-define(LOG_UTIL, log_utilities).
-define(PARTITION_VNODE, specula_vnode).
-define(PARTITION_DOWNSTREAM, clocksi_downstream).
-endif.


%% API
-export([start_link/3, start_link/2]).

%% Callbacks
-export([init/1,
         stop/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([start_execute_txns/2,
         finish_op/3,
         receive_reply/2,
         single_committing/2]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------

-record(state, {
        %% Metadata for all transactions
        from :: {pid(), term()},
        txn_id_list :: [txid()],
        all_txn_ops :: [],
        specula_meta :: dict(),
        num_txns :: non_neg_integer(),
        current_txn_index :: non_neg_integer(),
        num_committed_txn :: non_neg_integer(),
        %% Metadata for a single txn
	    tx_id :: txid(),
        num_to_prepare = 0 :: non_neg_integer(),
        prepare_time = 0 :: non_neg_integer(),
        updated_parts = dict:new() :: dict(),
        read_set = [] :: [],

        causal_clock :: non_neg_integer(),
        %%Stat
        num_aborted :: non_neg_integer(),
        prepare_begin_list = [] :: [],
        prepare_stat = [] :: [],
        read_valid_stat = [] :: [],
        read_stat = [] :: []
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, 0, Operations], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Txns]) ->
    random:seed(now()),
    SD = #state{
            all_txn_ops = Txns,
            num_txns = length(Txns),
            current_txn_index = 1,
            updated_parts = dict:new(),
            specula_meta = dict:new(),
            txn_id_list = [],
            causal_clock = ClientClock,
            num_committed_txn = 0,
            num_aborted = 0,
            from = From
           },
    %%io:format(user, "Sending msg to myself ~w, from is ~w~n", [Self, From]),
    {ok, start_execute_txns, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
start_execute_txns(timeout, SD) ->
    process_txs(SD).

process_txs(SD=#state{causal_clock=CausalClock, num_committed_txn=CommittedTxn, 
        all_txn_ops=AllTxnOps, prepare_begin_list=PrepareBeginList,  
        current_txn_index=CurrentTxnIndex, num_txns=NumTxns}) ->
    TxId = tx_utilities:create_transaction_record(CausalClock),
    case NumTxns of
        0 ->
            proceed_txn(SD#state{prepare_time=TxId#tx_id.snapshot_time});
        _ -> 
            MyOperations = lists:nth(CurrentTxnIndex, AllTxnOps),
            PrecCommitted = (CommittedTxn == CurrentTxnIndex -1),
            {CanCommit, WriteSet, ReadSet, NumToPrepare, NewReadStat, PrepareBegin} = 
                        process_operations(TxId, MyOperations, dict:new(), [], dict:new(), 0, PrecCommitted, []),
            case CanCommit of
                true -> %%TODO: has to find some way to deal with read-only transaction
                    proceed_txn(SD#state{read_set=ReadSet, read_stat=NewReadStat, 
                        prepare_begin_list=[PrepareBegin|PrepareBeginList], prepare_stat=[], 
                        prepare_time=TxId#tx_id.snapshot_time, num_committed_txn=CommittedTxn+1, tx_id=TxId});
                false ->
                    {next_state, receive_reply, SD#state{num_to_prepare=NumToPrepare, read_set=ReadSet,
                        updated_parts=WriteSet, tx_id=TxId, read_stat=NewReadStat, prepare_stat=[], 
                        prepare_begin_list=[PrepareBegin|PrepareBeginList]}, ?SPECULA_TIMEOUT}
            end
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_reply(timeout,
                 S0=#state{num_to_prepare=_NumToPrepare, num_aborted=NumAborted, tx_id=_TxId}) ->
    case specula_utilities:coord_should_specula(NumAborted) of
        true ->
            %%%%lager:info("Timeouted, coordinator proceed.. ~w", [TxId]),
            proceed_txn(S0);
        false ->
            %%%%lager:info("Timeouted, coordinator still not proceeding.. ~w", [TxId]),
            {next_state, receive_reply, S0}
    end;

receive_reply({Type, CurrentTxId, Param},
                 S0=#state{tx_id=CurrentTxId,
                           num_committed_txn = NumCommittedTxn,
                           num_to_prepare=NumToPrepare,
                           current_txn_index=CurrentTxnIndex,
                           updated_parts=UpdatedParts,
                           num_aborted=NumAborted,
                           prepare_stat=PrepareStat,
                           prepare_begin_list=PrepareBeginList,
                           prepare_time=PrepareTime}) ->
    PrepareTime1 = max(PrepareTime, Param), 
    NumToPrepare1 = NumToPrepare-1,
    NewPrepareStat = case Type of 
                        prepared -> 
                            [tx_utilities:now_microsec()-hd(PrepareBeginList)|PrepareStat];
                        _ ->
                            PrepareStat 
                    end,
    case can_commit(NumToPrepare1, NumCommittedTxn, CurrentTxnIndex) of
        true ->
            %case Type of
            %    read_valid ->
            %        lager:info("Current ~w can commit! Index is ~w",[CurrentTxId, CurrentTxnIndex]);
            %    _ ->
            %        ok
            %end,
            ?PARTITION_VNODE:commit(UpdatedParts, CurrentTxId, 
                        PrepareTime1),
            proceed_txn(S0#state{num_committed_txn=NumCommittedTxn+1, prepare_time=PrepareTime1, prepare_stat=NewPrepareStat});
        false ->
           %io:format(user, "Can not commit ~w, is curren!~n", [CurrentTxId]),
           %%%%lager:info("~w:C can not commit!",[CurrentTxId]),
            case specula_utilities:coord_should_specula(NumAborted) of
                true ->
                    proceed_txn(S0#state{prepare_time=PrepareTime1, num_to_prepare=NumToPrepare1, prepare_stat=NewPrepareStat});
                false ->
                    {next_state, receive_reply,
                     S0#state{prepare_time=PrepareTime1, num_to_prepare=NumToPrepare1, prepare_stat=NewPrepareStat}}
            end
    end;
receive_reply({Type, TxId, Param},
                 S0=#state{tx_id=CurrentTxnId,
                           num_committed_txn = NumCommittedTxn,
                           txn_id_list=TxIdList,
                           current_txn_index=CurrentTxnIndex,
                           num_to_prepare=CurrentNumToPrepare,
                           prepare_time=CurrentPrepareTime,
                           updated_parts=UpdatedParts,
                           prepare_begin_list=PrepareBeginList,
                           specula_meta=SpeculaMeta}) ->
    case dict:find(TxId, SpeculaMeta) of
        {ok, TxnMeta} ->
            %io:format(user, "Got something ~w for ~w, num_committed txn is ~w, not current!~n", [Type, TxId, NumCommittedTxn]),
            PrepareTime1 = max(TxnMeta#txn_metadata.prepare_time, Param), 
            NumToPrepare1 = TxnMeta#txn_metadata.num_to_prepare - 1,
            TxnIndex = TxnMeta#txn_metadata.index, 
            TxnMeta1 = case Type of 
                        prepared -> 
                            OldPrepareStat = TxnMeta#txn_metadata.prepare_stat,
                            PrepareLength = tx_utilities:now_microsec()-
                                        lists:nth(CurrentTxnIndex-TxnIndex+1, 
                                        PrepareBeginList),
                            TxnMeta#txn_metadata{prepare_time=PrepareTime1, num_to_prepare=NumToPrepare1,
                                prepare_stat=[PrepareLength|OldPrepareStat]};
                        _ ->
                            TxnMeta#txn_metadata{prepare_time=PrepareTime1, num_to_prepare=NumToPrepare1}
                        end,
            case can_commit(NumToPrepare1, NumCommittedTxn, TxnIndex) of
                true -> 
                    NewNumCommitted = 
                            cascading_commit_tx(TxId, TxnMeta1, SpeculaMeta, TxIdList),
                    case can_commit(CurrentNumToPrepare, NewNumCommitted, CurrentTxnIndex) of
                        true ->
                            SpeculaMeta1 = dict:store(TxId, TxnMeta1, SpeculaMeta),
                            ?PARTITION_VNODE:commit(UpdatedParts, CurrentTxnId, 
                                    CurrentPrepareTime),
                            proceed_txn(S0#state{num_committed_txn=NewNumCommitted+1, specula_meta=SpeculaMeta1});
                        false ->
                            %lager:info("Current ~w can not commit, num of committed is ~w!",[CurrentTxnId, NewNumCommitted]),
                            SpeculaMeta1 = dict:store(TxId, TxnMeta1, SpeculaMeta),
                            {next_state, receive_reply, 
                                S0#state{num_committed_txn=NewNumCommitted, specula_meta=SpeculaMeta1}} 
                    end;
                false ->
                    %case Type of
                    %    read_valid ->
                    %        lager:info("~w can not commit! Index is ~w, num to prepare is ~w, num_committed is ~w",[TxId, TxnMeta1#txn_metadata.index, TxnMeta1#txn_metadata.num_to_prepare, NumCommittedTxn]);
                    %    _ ->
                    %        ok
                    %end,
                    %lager:info("~w can not commit! Old num is ~w",[TxId, NumCommittedTxn]),
                    %io:format(user, "Can not commit ~w, not current!~n", [TxId]),
                    SpeculaMeta1 = dict:store(TxId, TxnMeta1, SpeculaMeta),
                    {next_state, receive_reply, S0#state{specula_meta=SpeculaMeta1}} 
            end;
        error ->
            {next_state, receive_reply, S0}
    end;

%% Abort due to invalid read or invalid prepare
receive_reply({abort, TxId}, S0=#state{tx_id=CurrentTxId, specula_meta=SpeculaMeta, current_txn_index=_Index,
                 num_aborted=NumAborted, updated_parts=UpdatedParts}) ->
    case TxId of
        CurrentTxId ->
            %lager:info("Aborting current tx~w of index ~w", [CurrentTxId, Index]),
            ?PARTITION_VNODE:abort(UpdatedParts, CurrentTxId),
            timer:sleep(random:uniform(?DUMB_TIMEOUT)),
            %% Restart from current transaction.
            process_txs(S0#state{num_aborted=NumAborted+1});
        _ ->
            case dict:find(TxId, SpeculaMeta) of
                {ok, AbortTxnMeta} ->
                    %lager:info("Aborting other tx ~w of index ~w, current index ~w", [TxId, AbortTxnMeta#txn_metadata.index, Index]),
                    S1 = cascading_abort(AbortTxnMeta, S0),
                    timer:sleep(random:uniform(?DUMB_TIMEOUT)),
                    process_txs(S1#state{num_aborted=NumAborted+1});
                error ->
                    %%lager:warning("Can't find txn wants to abort!!! ~w",[TxId]),
                    {next_state, receive_reply, S0}
            end
    end.

%% Abort because prepare failed (concurrent txs has committed or prepared).
%receive_reply(abort, S0=#state{tx_id=CurrentTxId, current_txn_meta=CurrentTxnMeta}) ->
%    %%%%lager:info("Receive aborted for current tx is ~w", [CurrentTxId]),
%    ?PARTITION_VNODE:abort(CurrentTxnMeta#txn2_metadata.updated_parts, CurrentTxId),
%    proceed_txn(S0#state{state=aborted}).

%% TODO: need to define better this case: is specula_committed allowed, or not?
%% Why this case brings doubt???
single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    proceed_txn(S0#state{prepare_time=CommitTime});
    
single_committing(abort, S0=#state{from=_From}) ->
    proceed_txn(S0).


%% @doc proceed_txn is called when timeout has expired, a transaction is aborted or 
%%      the current transaction is committed.
proceed_txn(S0=#state{from=From, tx_id=TxId, txn_id_list=TxIdList, current_txn_index=CurrentTxnIndex,
             num_committed_txn=NumCommittedTxn, prepare_time=MaxPrepTime, read_set=ReadSet,
             num_to_prepare=NumToPrepare, updated_parts=UpdatedParts, read_stat=ReadStat, prepare_stat=PrepareStat, 
             specula_meta=SpeculaMeta, num_txns=NumTxns}) ->
    case NumTxns of
        %% The last txn has already committed
        NumCommittedTxn ->
            {AllReadSet, AllPrepareStat, AllReadStat} = get_list_meta(TxIdList, SpeculaMeta, [], [], []),
            AllReadSet1 = [ReadSet|AllReadSet],
            AllPrepareStat1 = [PrepareStat|AllPrepareStat],
            AllReadStat1 = [ReadStat|AllReadStat],
            %lager:info("ReadStat is ~w, prepareStat is ~w", [ReadStat, AllPrepareStat1]),
            stat_server:send_stat(lists:reverse(lists:flatten(AllReadStat1)), lists:reverse(lists:flatten(AllPrepareStat1))),
            From ! {ok, {TxId, lists:reverse(lists:flatten(AllReadSet1)), 
                MaxPrepTime}},
            {stop, normal, S0};
        %% In the last txn but has not committed
        CurrentTxnIndex ->
            {next_state, receive_reply, S0};
        %% Proceed
        _ -> 
            SpeculaMeta1= dict:store(TxId, #txn_metadata{read_set=ReadSet, prepare_time=MaxPrepTime, prepare_stat=PrepareStat, 
                        index=CurrentTxnIndex, num_to_prepare=NumToPrepare, read_stat=ReadStat, updated_parts=UpdatedParts}, 
                            SpeculaMeta),
            TxIdList1 = TxIdList ++ [TxId],
            %lager:info("proceed with max prepe time ~w", [MaxPrepTime]),
            process_txs(S0#state{specula_meta=SpeculaMeta1, current_txn_index=CurrentTxnIndex+1, num_to_prepare=0, 
                prepare_time=0, read_set=[], updated_parts=dict:new(),
                txn_id_list=TxIdList1, causal_clock=max(TxId#tx_id.snapshot_time, MaxPrepTime)})
    end.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(stop,_From,_StateName, StateData) ->
    %lager:info("~w: stopped", [self()]),
    {stop,normal,ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%%% Private function %%%%%%%%%%

process_operations(TxId, [], WriteSet, ReadSet, _, ReadDep, CanCommit, ReadStat) ->
    case dict:size(WriteSet) of
        0 ->
            {CanCommit, WriteSet, ReadSet, ReadDep, ReadStat, 0};
        N ->
            %lager:info("Send write set ~w",[WriteSet]),
            PrepareBegin = tx_utilities:now_microsec(),
            ?PARTITION_VNODE:prepare(WriteSet, TxId),
            {CanCommit, WriteSet, ReadSet, N+ReadDep, ReadStat, PrepareBegin}
    end;
process_operations(TxId, [{read, Key, Type}|Rest], UpdatedParts, RSet, Buffer, ReadDep, CanCommit, ReadStat) ->
    ReadBegin = tx_utilities:now_microsec(),
    Reply = case dict:find(Key, Buffer) of
                    error ->
                        Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                        IndexNode = hd(Preflist),
                        ?PARTITION_VNODE:read_data_item(IndexNode, Key, Type, TxId);
                    {ok, SnapshotState} ->
                        {ok, SnapshotState}
            end,
    ReadLength = tx_utilities:now_microsec() - ReadBegin,
    %%%%lager:info("Read got reply ~w",[Reply]),
    {NewReadDep, CanCommit1} = case Reply of
                                    {specula, _Snapshot} ->
                                        %lager:info("~w: ~w speculative", [TxId, Key]),
                                        {ReadDep+1, false};
                                    _ ->
                                        {ReadDep, CanCommit}
                               end,
    {_, KeySnapshot} = Reply,
    %io:format(user, "~nType is ~w, Key is ~w, Snapshot is ~w, ~n",[Type, Key, KeySnapshot]),
    Buffer1 = dict:store(Key, KeySnapshot, Buffer),
    process_operations(TxId, Rest, UpdatedParts, 
            [Type:value(KeySnapshot)|RSet], Buffer1, NewReadDep, CanCommit1, [ReadLength|ReadStat]);
process_operations(TxId, [{update, Key, Type, Op}|Rest], UpdatedParts, RSet, Buffer, ReadDep, _, ReadStat) ->
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    UpdatedParts1 = case dict:is_key(IndexNode, UpdatedParts) of
                        false ->
                            dict:store(IndexNode, [{Key, Type, Op}], UpdatedParts);
                        true ->
                            dict:append(IndexNode, {Key, Type, Op}, UpdatedParts)
                    end,
    Buffer1 = case dict:find(Key, Buffer) of
                error ->
                    Init = Type:new(),
                    {Param, Actor} = Op,
                    {ok, NewSnapshot} = Type:update(Param, Actor, Init),
                    dict:store(Key, NewSnapshot, Buffer);
                {ok, Snapshot} ->
                    {Param, Actor} = Op,
                    {ok, NewSnapshot} = Type:update(Param, Actor, Snapshot),
                    dict:store(Key, NewSnapshot, Buffer)
                end,
    process_operations(TxId, Rest, UpdatedParts1, RSet, Buffer1, ReadDep, false, ReadStat).

cascading_abort(AbortTxnMeta, #state{tx_id=CurrentTxId, updated_parts=UpdatedParts, 
                             specula_meta=SpeculaMeta,txn_id_list=TxIdList}=S0) ->
    AbortIndex = AbortTxnMeta#txn_metadata.index,
    AbortTxList = lists:nthtail(AbortIndex-1, TxIdList),
    %lager:info("Cascading abort.. TxIdList is ~w, dict key ~w", [TxIdList, dict:fetch_keys(SpeculaMeta)]),
    AbortFun = fun(Id, Dict) -> 
                                TMeta = dict:fetch(Id, Dict),  
                                ?PARTITION_VNODE:abort(TMeta#txn_metadata.updated_parts, Id),
                                dict:erase(Id, Dict)
                end, 
    SpeculaMeta1 = lists:foldl(AbortFun, SpeculaMeta, AbortTxList),
    
    %Abort current transaction
    ?PARTITION_VNODE:abort(UpdatedParts, CurrentTxId),

    S0#state{txn_id_list=lists:sublist(TxIdList, AbortIndex-1),
                current_txn_index=AbortIndex, specula_meta=SpeculaMeta1}.

cascading_commit_tx(TxId, TxnMeta, SpeculaMeta, TxIdList) ->
    %%%%lager:info("Doing cascading commit ~w",[TxId]),
    ?PARTITION_VNODE:commit(TxnMeta#txn_metadata.updated_parts, TxId, 
                TxnMeta#txn_metadata.prepare_time),
    Index = TxnMeta#txn_metadata.index,
    %lager:info("~w committed ~w", [TxId, Index]),
    try_commit_successors(lists:nthtail(Index, TxIdList), SpeculaMeta, Index).

try_commit_successors([], _SpeculaMetadata, Index) ->
    Index;
try_commit_successors([TxId|Rest], SpeculaMetadata, Index) ->
    TxnMeta = dict:fetch(TxId, SpeculaMetadata),
    case TxnMeta#txn_metadata.num_to_prepare of
        0 ->
            %lager:info("~w committed with ~w", [TxId, TxnMeta#txn_metadata.prepare_time]),
            ?PARTITION_VNODE:commit(TxnMeta#txn_metadata.updated_parts, 
                    TxId, TxnMeta#txn_metadata.prepare_time),
            try_commit_successors(Rest, SpeculaMetadata, Index+1);
        _N ->
            %lager:info("~w can not commit, index is ~w, prep is ~w", [TxId, Index+1, N]),
            Index
    end.

can_commit(NumToPrepare, NumCommittedTxn, TxnIndex) ->
    case NumToPrepare of
        0 ->
            %io:format(user, "Committed is ~w, index is ~w~n", [NumCommittedTxn, TxnMeta#txn_metadata.index]),
            NumCommittedTxn == TxnIndex - 1;
        _N ->
            %lager:info("Can not commit, TxnIndex is ~w, P left is ~w",[TxnIndex, N]),
            false
    end.

get_list_meta([], _SpeculaMeta, Acc1, Acc2, Acc3) ->
    {Acc1, Acc2, Acc3};
get_list_meta([H|T], SpeculaMeta, Acc1, Acc2, Acc3) ->
    Tx = dict:fetch(H, SpeculaMeta),
    get_list_meta(T, SpeculaMeta, [Tx#txn_metadata.read_set|Acc1], [Tx#txn_metadata.prepare_stat|Acc2],
                    [Tx#txn_metadata.read_stat|Acc3]).


-ifdef(SKIP).
process_op_test() ->
    %% Common read
    TxId = tx_utilities:create_transaction_record(tx_utilities:now_microsec()),
    Key1 = {counter, 1},
    Key2 = {counter, 2},
    Type = riak_dt_gcounter,
    Op1 = {increment, haha},
    Operations = [{read, Key1, Type}, {update, Key1, Type, Op1}, {read, Key1, Type}, {read, Key2, Type},
                {read, Key2, Type}],
    {_, _, ReadSet, NumToPrepare, _, _} = process_operations(TxId, Operations,
            dict:new(), [], dict:new(), 0, true, []),

    ?assertEqual([2, 3, 2, 2], lists:reverse(ReadSet)),
    ?assertEqual(1, NumToPrepare),

    %% Read with dependency
    Key3 = {specula, 1, 10},
    Operations1 = [{read, Key1, Type}, {update, Key1, Type, Op1}, {read, Key1, Type}, {read, Key2, Type},
                {read, Key2, Type}, {read, Key3, Type}],
    {_, _, ReadSet2, NumToPrepare2, _, _} = process_operations(TxId, Operations1,
            dict:new(), [], dict:new(), 0, true, []),
   
    ?assertEqual([2, 3, 2, 2, 2], lists:reverse(ReadSet2)),
    ?assertEqual(2, NumToPrepare2).

    
cascading_abort_test() ->
    State = generate_specula_meta(4, 3, 1, 1),
    [TxId1, TxId2, _] = State#state.txn_id_list,
    TxId2Meta = dict:fetch(TxId2, State#state.specula_meta),
    State1 = cascading_abort(TxId2Meta, State),
    TxnIndex = State1#state.current_txn_index,
    RemainingTxId = State1#state.txn_id_list,
    ?assertEqual(TxnIndex, 2),
    ?assertEqual(RemainingTxId, [TxId1]).

cascading_commit_test() ->
    State = generate_specula_meta(5, 3, 4, 0),
    [TxId1, TxId2, TxId3] = State#state.txn_id_list,
    SpeculaMeta = State#state.specula_meta,

    Tx1Meta = dict:fetch(TxId1, SpeculaMeta),
    Committed1 = cascading_commit_tx(TxId1, Tx1Meta, SpeculaMeta, [TxId1, TxId2, TxId3]),
    ?assertEqual(3, Committed1),

    %% Without committing the first..
    Tx2Meta = dict:fetch(TxId2, SpeculaMeta),
    Committed2 = cascading_commit_tx(TxId2, Tx2Meta, SpeculaMeta, [TxId1, TxId2, TxId3]),
    ?assertEqual(3, Committed2),

    State1 = generate_specula_meta(5, 4, 2, 1),
    TxnList1 = State1#state.txn_id_list,
    [_TxId11, TxId12, _TxId13, _Txn14] = TxnList1, 
    SpeculaMeta1 = State1#state.specula_meta,

    TxMeta12 = dict:fetch(TxId12, SpeculaMeta1),
    Committed3 = cascading_commit_tx(TxId12, TxMeta12, SpeculaMeta1, TxnList1),
    ?assertEqual(2, Committed3).
    
can_commit_test() ->
    State = generate_specula_meta(4,3,2,1),
    SpeculaMeta = State#state.specula_meta,
    [TxId1, TxId2, TxId3] = State#state.txn_id_list,
    _TxMeta1 = dict:fetch(TxId1, SpeculaMeta), 
    _TxMeta2 = dict:fetch(TxId2, SpeculaMeta), 
    _TxMeta3 = dict:fetch(TxId3, SpeculaMeta), 
%    ?assertEqual(true, can_commit(TxMeta1, SpeculaMeta, State#state.txn_id_list)),
%    ?assertEqual(true, can_commit(TxMeta2, SpeculaMeta, State#state.txn_id_list)),
%    ?assertEqual(false, can_commit(TxMeta3, SpeculaMeta, State#state.txn_id_list)).
    ok.

generate_specula_meta(NumTotalTxn, NumSpeculaTxn, NumCanCommit, _NumCommitted) ->
    Seq0 = lists:seq(1, NumTotalTxn),
    AllTxnOps = lists:map(fun(_) -> generate_random_op(1, 1, []) end, Seq0),

    %% TxIds of specually committed transaction
    Seq1 = lists:seq(1, NumSpeculaTxn),
    TxnIdList = lists:map(fun(_) -> tx_utilities:create_transaction_record(
                        tx_utilities:now_microsec()) end, Seq1),

    %% Metadatas of txns that can finally commit
    Seq2 = lists:seq(1, NumCanCommit),
    CanCommitTxn = lists:map(fun(_) -> generate_txn_meta(false, false) end, Seq2),

    {TxnList, [CurrentTxn]} = case NumSpeculaTxn >= NumCanCommit of
                        true ->
                            PendingTxn = lists:map(fun(_) -> generate_txn_meta(false, true) end,
                                lists:seq(1, NumSpeculaTxn - NumCanCommit+1)),
                            {PendingTxn1, Txn2} = lists:split(length(PendingTxn) - 1, PendingTxn),
                            {CanCommitTxn++PendingTxn1, Txn2};
                        false ->
                            lists:split(length(CanCommitTxn) - 1, CanCommitTxn)
                        end,

    Zipped = lists:zip(TxnIdList, TxnList),
    {SpeculaMeta, _} = lists:foldl(fun({Id, Txn}, {Dict, Acc}) ->
                {dict:store(Id, Txn#txn_metadata{index=Acc}, Dict), Acc+1} end, {dict:new(),1}, Zipped),

    CurrentTxId = tx_utilities:create_transaction_record(tx_utilities:now_microsec()),

    #state{all_txn_ops=AllTxnOps, txn_id_list=TxnIdList, tx_id=CurrentTxId, 
        num_to_prepare=CurrentTxn#txn_metadata.num_to_prepare, current_txn_index=NumSpeculaTxn+1, 
            specula_meta=SpeculaMeta}.

generate_txn_meta(false, false) ->
    #txn_metadata{num_to_prepare=0};
generate_txn_meta(PendingRead, PendingPrepare) ->
    ReadDep = case PendingRead of true -> 2; false -> 0 end,
    NumUpdated = case PendingPrepare of true -> 1; false -> 0 end,
    #txn_metadata{num_to_prepare=NumUpdated+ReadDep}.


generate_random_op(0, 0, Acc) ->
    Acc;
generate_random_op(0, N, Acc) ->
    Key = {counter, random:uniform(10)},
    generate_random_op(0, N-1,
            [{update, Key, riak_dt_gcounter, {increment, random:uniform(100)}}|Acc]);
generate_random_op(N, 0, Acc) ->
    Key = {counter, random:uniform(10)},
    generate_random_op(N-1, 0, [{read, Key, riak_dt_gcounter}|Acc]);
generate_random_op(NumRead, NumWrite, Acc) ->
    Key = {counter, random:uniform(10)},
    case random:uniform(10) rem 2 of
        0 ->
            generate_random_op(NumRead-1, NumWrite, [{read, Key, riak_dt_gcounter}|Acc]);
        1 ->
            generate_random_op(NumRead, NumWrite-1,
                    [{update, Key, riak_dt_gcounter, {increment, random:uniform(100)}}|Acc])
    end.

main_test_() ->
    {foreach,
     fun setup/0,
     [
      fun empty_test/1,
      fun single_txn_single_read_test/1,
      fun single_txn_multi_read_test/1,
      fun single_txn_mixed_test/1,

      fun multi_txn_single_read_test/1,
      fun multi_txn_multi_read_test/1,

      fun multi_txn_read_dep_test/1,
      fun multi_txn_multi_read_dep_test/1,
      fun multi_txn_wait_test/1,
      fun multi_txn_multi_wait_test/1
%      fun update_single_success_test/1,
%      fun update_multi_abort_test1/1,
%      fun update_multi_abort_test2/1,
%      fun update_multi_success_test/1,

%      fun read_single_fail_test/1,
%      fun read_success_test/1

     ]}.

% Setup and Cleanup
setup()      -> ignore. 

empty_test(_) ->
    fun() ->
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, []), 
            receive Msg ->
                ?assertMatch({ok, {_, [], _}}, Msg)
            end
    end.

single_txn_single_read_test(_) ->
    fun() ->
            Key = counter,
            Type = riak_dt_gcounter,
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, [[{read, Key, Type}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2], _}}, Msg)
            end
    end.

single_txn_multi_read_test(_) ->
    fun() ->
            Key1 = {counter,1},
            Key2 = {counter,2},
            Type = riak_dt_gcounter,
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, 
                    [[{read, Key1, Type}, {read, Key2, Type}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,2], _}}, Msg)
            end
    end.

single_txn_mixed_test(_) ->
    fun() ->
            Key1 = {counter,1},
            Type = riak_dt_gcounter,
            Param = {increment, noone},
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, 
                    [[{read, Key1, Type}, {update, Key1, Type, Param}, {read, Key1, Type}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,3], _}}, Msg)
            end
    end.

multi_txn_single_read_test(_) ->
    fun() ->
            Key = counter,
            Type = riak_dt_gcounter,
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, [[{read, Key, Type}],
                    [{read, Key, Type}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,2], _}}, Msg)
            end
    end.

multi_txn_multi_read_test(_) ->
    fun() ->
            Key1 = {counter,1},
            Key2 = {counter,2},
            Type = riak_dt_gcounter,
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, 
                    [[{read, Key1, Type}, {read, Key2, Type}], [{read, Key1, Type}, {read, Key2, Type}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,2,2,2], _}}, Msg)
            end
    end.

multi_txn_read_dep_test(_) ->
    fun() ->
            Key1 = {counter,1},
            Key2 = {counter,2},
            SpeculaKey1 = {specula, 100, 100},
            Type = riak_dt_gcounter,
            Param = {increment, noone},
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, 
                    [[{read, SpeculaKey1, Type}, {read, Key1, Type}], [{update, Key2, Type, Param}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,2], _}}, Msg)
            end
    end.

multi_txn_multi_read_dep_test(_) ->
    fun() ->
            SpeculaKey1 = {specula, 100, 500},
            SpeculaKey2 = {specula, 100, 300},
            SpeculaKey3 = {specula, 100, 100},
            Type = riak_dt_gcounter,
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, 
                    [[{read, SpeculaKey1, Type}], 
                    [{read, SpeculaKey2, Type}], 
                    [{read, SpeculaKey3, Type}]]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,2,2], _}}, Msg)
            end
    end.

multi_txn_wait_test(_) ->
    fun() ->
            Key1 = {counter,1},
            Key2 = {counter,2},
            WaitKey = {wait, 100},
            SpeculaKey1 = {specula, 100, 100},
            Type = riak_dt_gcounter,
            Param = {increment, noone},
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, [ 
                    [{read, SpeculaKey1, Type}, {read, Key1, Type}], 
                    [{update, WaitKey, Type, Param}],
                    [{read, Key2, Type}, {update, Key2, Type, Param}]
                    ]), 
            receive Msg ->
                ?assertMatch({ok, {_, [2,2,2], _}}, Msg)
            end
    end.

multi_txn_multi_wait_test(_) ->
    fun() ->
            WaitKey1 = {wait, 500},
            WaitKey2 = {wait, 300},
            WaitKey3 = {wait, 100},
            Type = riak_dt_gcounter,
            Param = {increment, noone},
            {ok, _Pid} = specula_general_tx_coord_fsm:start_link(self(), 0, [ 
                    [{update, WaitKey1, Type, Param}], 
                    [{update, WaitKey2, Type, Param}], 
                    [{update, WaitKey3, Type, Param}]
                    ]), 
            receive Msg ->
                ?assertMatch({ok, {_, [], _}}, Msg)
            end
    end.


-endif.