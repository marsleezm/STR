% -------------------------------------------------------------------
%
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
%% @doc This file implements functions for performing certification
%% check, committing transactions, aborting transactions and checking
%% whether a transactions can (speculatively) read a record. 
%%
-module(local_cert_util).

-include("include/speculation.hrl").
-include("include/antidote.hrl").

-export([prepare_for_master_part/5, 
        ready_or_block/4, 
        prepare_for_other_part/7, 
        update_store/9, 
        clean_abort_prepared/7, 
        local_commit/10, 
        insert_prepare/6, 
        specula_read/4, 
        read_value/4, 
        remote_read_value/4]).

%% @doc Performs certification check for a transaction in a master partition
%% and decides whether to commit or abort this transaction. 
prepare_for_master_part(TxId, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime)->
    KeySet = [K || {K, _} <- TxWriteSet],
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0, 0) of
        false ->
            {error, write_conflict};
        %% Directly prepare
        {0, 0, PrepareTime} ->
          %lager:warning("~p passed prepare with ~p", [TxId, PrepareTime]),
            lists:foreach(fun({K, V}) ->
                    ets:insert(PreparedTxs, {K, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {TxId, KeySet}),
            {ok, PrepareTime};
        %% Pend-prepare. 
        {PendPrepDep, PrepDep, PrepareTime} ->
          %lager:warning("~p passed but has ~p pend dep, ~p prepdep, prepare with ~p", [TxId, PendPrepDep, PrepDep, PrepareTime]),
            lists:foreach(fun({K, V}) ->
                          case ets:lookup(PreparedTxs, K) of
                          [] ->
                              ets:insert(PreparedTxs, {K, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]});
                          [{K, {LastReaderTime, FirstPrepTime, PrepNum}, [{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|Rest]}] ->
                              ets:insert(PreparedTxs, {K, {LastReaderTime, FirstPrepTime, PrepNum+1}, [{prepared, TxId, PrepareTime, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|Rest]]});
                          _R -> 
                            ets:insert(PreparedTxs, {K, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
                          end
                    end, TxWriteSet),
            ets:insert(PreparedTxs, {TxId, KeySet}),
            {wait, PendPrepDep, PrepDep, PrepareTime}
    end.

%% @doc Performs certification check for a transaction in a cache/slave partition
%% and decides whether to commit or abort this transaction. 
prepare_for_other_part(TxId, Partition, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime, PartitionType)->
    KeySet = case PartitionType of cache -> [{Partition,K} || {K, _} <- TxWriteSet];
                                   slave -> [K || {K, _} <- TxWriteSet]
             end,
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0, 0) of
        false ->
            {error, write_conflict};
        %% Directly prepare
        {0, 0, PrepareTime} ->
            %lager:warning("~p passed prepare with ~p, KeySet is ~p", [TxId, PrepareTime, KeySet]),
            lists:foreach(fun({K, V}) ->
                    InsertKey = case PartitionType of cache -> {Partition, K}; slave -> K end,
                    ets:insert(PreparedTxs, {InsertKey, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
            {ok, PrepareTime};
        %% Pend-prepare. 
        {PendPrepDep, PrepDep, PrepareTime} ->
          %lager:warning("~p passed but has ~p pend prep deps, ~p prep dep, prepare with ~p, KeySet is ~w", [TxId, PendPrepDep, PrepDep, PrepareTime, KeySet]),
            lists:foreach(fun({K, V}) ->
                InsertKey = case PartitionType of cache -> {Partition, K}; slave -> K end,
                case ets:lookup(PreparedTxs, InsertKey) of
                [] -> 
                    ets:insert(PreparedTxs, {InsertKey, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]});
                [{InsertKey,  {LastRTime, FirstPrepTime, PrepNum},[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]}] ->
                    ets:insert(PreparedTxs, {InsertKey, {LastRTime, FirstPrepTime, PrepNum+1}, [{prepared, TxId, PrepareTime, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]]});
                _R -> 
                    ets:insert(PreparedTxs, {InsertKey, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
                end
            end, TxWriteSet),
            true = ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
            {wait, PendPrepDep, PrepDep, PrepareTime}
    end.

%% @doc: Certification check for each key. 
%% Abort a transaction if there is a conflicting committed/local-committed/prepared transaction 
%% for this key; if there is a non-conflicting local-committed/prepared transaction, block this
%% transactions; otherwise, this key passes certification check. 
%% In the first case, the transaction gets directly aborted; in the latter two, continue checking
%% for the other keys.
certification_check(FinalPrepTime, _, [], _, _, PendPrepDep, PrepDep) ->
    {PendPrepDep, PrepDep, FinalPrepTime};
certification_check(PrepareTime, TxId, [Key|T], CommittedTxs, PreparedTxs, PendPrepDep, PrepDep) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case CommittedTxs of
        ignore ->
            case check_prepared(TxId, PreparedTxs, Key, whatever) of
                {true, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep);
                {pend_prep_dep, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep+1, PrepDep);
                {prep_dep, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep+1);
                false ->
                     %lager:warning("~p: False of prepared for ~p", [TxId, Key]),
                    false
            end;
        _ ->
            case ets:lookup(CommittedTxs, Key) of
                [{Key, CommitTime}] ->
                    case CommitTime > SnapshotTime of
                        true -> false;
                            %lager:warning("False for committed key ~p, Snapshot is ~p, diff with commit ~p", [Key, TxId#tx_id.snapshot_time, CommitTime-TxId#tx_id.snapshot_time]),
                        false ->
                            case check_prepared(TxId, PreparedTxs, Key, whatever) of
                                {true, NewPrepTime} ->
                                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep);
                                {pend_prep_dep, NewPrepTime} ->
                                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep+1, PrepDep);
                                {prep_dep, NewPrepTime} ->
                                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep+1);
                                false ->
                                    false
                            end
                    end;
                [] ->
                    case check_prepared(TxId, PreparedTxs, Key, whatever) of
                        {true, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep);
                        {pend_prep_dep, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep+1, PrepDep);
                        {prep_dep, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep+1);
                        false ->
                             %lager:warning("~p: False of prepared for ~p", [TxId, Key]),
                            false
                    end
            end
    end.

%% @doc: Doing certification check against prepared/local-committed transaction for this transaction
%% in the current key. 
check_prepared(TxId, PreparedTxs, Key, _Value) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            %ets:insert(PreparedTxs, {Key, [{TxId, PPTime, PPTime, PPTime, Value, []}]}),
            {true, 1};
        [{Key, {LastReaderTime, _FirstPrepTime, PrepNum}, [{_Type, _PrepTxId, _PrepareTime, _PrepValue, _RWaiter}|_PWaiter]=_Record}] ->
            %lager:warning("For key ~p: record is ~w! LastReaderTime is ~w, FirstPrepTime is ~p, ~p may fail, PrepNum is ~w", [Key, _Record, _PrepTxId, _FirstPrepTime, TxId, PrepNum]),
            %% Safety check. If this is false, means there is implemention problem. 
            case _PWaiter of
                [] -> _FirstPrepTime = _PrepareTime;
                _ -> ok
            end,
            case _PrepareTime > SnapshotTime of
                true ->
                    false;
                false ->
                   %lager:warning("~p: ~p waits for ~p that is ~p with ~p, PrepNum is ~w", [Key, TxId, _PrepTxId, _Type, _PrepareTime, PrepNum]),
                    %% If all previous txns have local committed, then this txn can be directly pend-prepared and specula-committed 
                    %% Otherwise, this txn has to wait until all preceding prepared txn to be local committed
                    %% has_spec_commit means this txn can pend prep and then spec commit 
                    %% has_pend_prep basically means this txn can not even pend_prep
                    case _PWaiter of
                        [{_,_,F,_,_}] -> _FirstPrepTime = F;
                        _ -> ok
                    end,
                    case PrepNum of 0 ->  {prep_dep, LastReaderTime+1};
                                    _ -> {pend_prep_dep, LastReaderTime+1}
                    end
            end;
        [{Key, LastReaderTime}] ->
            {true, LastReaderTime+1}
    end.

%% @doc: Commit a transaction by moving its prepared/local-committed records to committed state.
%% Meanwhile, all transactions that are blocked on it are unblocked, all pending readers are
%% unblocked and sent appropriate versions to read.
-spec update_store(Keys :: [{key()}], TxId::txid(), TxCommitTime:: {term(), term()}, InMemoryStore :: cache_id(), 
         CommittedTxs :: cache_id(), PreparedTxs :: cache_id(), DepDict :: dict(),  Partition :: integer(), 
            PartitionType :: term() ) -> dict().
update_store([], _TxId, _TxCommitTime, _InMemoryStore, _CommittedTxs, _PreparedTxs, DepDict, _Partition, _PartitionType) ->
    DepDict;
update_store([Key|Rest], TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PartitionType) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {LRTime, _FirstPrepTime, _PrepNum}, [{Type, TxId, _PrepareTime, MValue, PendingReaders}|Others]}] ->
            %lager:warning("~p Pending readers are ~p! Others are ~p", [TxId, PendingReaders, Others]),
            AllPendingReaders = lists:foldl(fun({_, _, _, _ , Readers}, CReaders) ->
                                       Readers++CReaders end, PendingReaders, Others), 
            Value = case Type of local_commit -> {_, _, V}=MValue, V; _ -> MValue end,
           %lager:warning("Trying to insert key ~p with for ~p, Type is ~p, prepnum is  is ~p, Commit time is ~p, MValue is ~w, pending readers are ~w", [Key, TxId, Type, _PrepNum, TxCommitTime, MValue, AllPendingReaders]),
            case PartitionType of
                cache ->  
                    %% If it is a cache partition, then send the appropriate version to pending readers,
                    %% if there is any; otherwise, the read request is sent to a remote replica.
                    lists:foreach(fun({ReaderTxId, Node, Sender}) ->
                            SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                            case SnapshotTime >= TxCommitTime of
                                true -> gen_server:reply(Sender, {ok, Value});
                                false ->
                                    {_, RealKey} = Key,
                                    master_vnode:remote_read(Node, RealKey, ReaderTxId, Sender)
                            end end, AllPendingReaders);
                _ ->
                    Values = case ets:lookup(InMemoryStore, Key) of
                                [] ->
                                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]}),
                                    [[], Value];
                                [{Key, ValueList}] ->
                                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                                    [{_CommitTime, First}|_] = RemainList,
                                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]}),
                                    [First, Value]
                            end,
                    ets:insert(CommittedTxs, {Key, TxCommitTime}),
                    lists:foreach(fun({ReaderTxId, Sender}) ->
                            RValue = case ReaderTxId#tx_id.snapshot_time >= TxCommitTime of
                                      true ->
                                          lists:nth(2,Values);
                                      false ->
                                          hd(Values)
                                    end,
                            case Sender of
                                {remote, Client} -> 
                                    %lager:warning("For ~w of reader ~w, replying to its server", [TxId, Client]),
                                    gen_server:cast(TxId#tx_id.server_pid, {rr_value, TxId, Client, TxCommitTime, RValue});
                                _ -> gen_server:reply(Sender, {ok, RValue})
                            end end,
                        AllPendingReaders)
            end,
            ets:insert(PreparedTxs, {Key, max(TxCommitTime, LRTime)}),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs,
                DepDict, Partition, PartitionType);
        [{Key, Metadata, Records}] ->
          %lager:warning("Trying to insert key ~p with for ~p, commit time is ~p, Records are ~p, PartitionType is ~p", [Key, TxId, TxCommitTime, Records, PartitionType]),
            DepDict2 = delete_and_read(commit, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, Records, TxId, [], Metadata, 0),
            case PartitionType of cache -> ok; _ -> ets:insert(CommittedTxs, {Key, TxCommitTime}) end,
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict2, Partition, PartitionType);
        _R ->
            %lager:error("For key ~w, txn ~w come first! Record is ~w", [Key, TxId, R]),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PartitionType)
    end.

%% @doc: abort a prepared/local-committed transaction.
%% Similarily, all transactions that are blocked on it are unblocked, all pending readers are
%% unblocked and sent appropriate versions to read.
clean_abort_prepared(_PreparedTxs, [], _TxId, _InMemoryStore, DepDict, _, _) ->
    DepDict; 
clean_abort_prepared(PreparedTxs, [Key | Rest], TxId, InMemoryStore, DepDict, Partition, PartitionType) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {LastReaderTime, FirstPPTime, PrepNum}, [{Type, TxId, _PrepTime, _Value, PendingReaders}|RestRecords]}] ->
           %lager:warning("Aborting ~p for key ~p, PrepNum is ~w, Type is ~w", [TxId, Key, PrepNum, Type]),
            case PendingReaders of
                [] -> ok;
                _ ->
                    case PartitionType of
                        cache -> 
                            lists:foreach(fun({ReaderTxId, Node, Sender}) -> 
                                {_, RealKey} = Key,
                                master_vnode:remote_read(Node, RealKey, ReaderTxId, Sender) end,
                            PendingReaders);
                        _ ->
                            {TS, RValue} = case ets:lookup(InMemoryStore, Key) of
                                                  [{Key, ValueList}] -> hd(ValueList);
                                                  [] -> {0, []}
                                           end,
                            lists:foreach(fun({RTx, Sender}) -> 
                                case Sender of
                                    {remote, Client} -> gen_server:cast(RTx#tx_id.server_pid, {rr_value, RTx, Client, TS, RValue}); 
                                    _ -> gen_server:reply(Sender, {ok, RValue})
                            end end, PendingReaders)
                    end
            end,
            case RestRecords of
                [] ->
                    true = ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType);
                [{HType, HTxId, HPTime, HValue, HReaders}|Others] ->
                    RPrepNum = case Type of local_commit -> PrepNum; _ -> PrepNum-1 end,
                    true = ets:insert(PreparedTxs, {Key, {LastReaderTime, FirstPPTime, RPrepNum}, [{HType, HTxId, HPTime, 
                            HValue, HReaders}|Others]}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
            end;
        [{Key, Metadata, Records}] -> 
           %lager:warning("Aborting TxId ~w, Key is ~p, metadata are ~w", [TxId, Key, Records]),
            DepDict2 = delete_and_read(abort, PreparedTxs, InMemoryStore, 0, Key, DepDict, PartitionType, Partition, Records, TxId, [], Metadata, 0),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition, PartitionType);
        _R ->
           %lager:warning("WTF? R is ~p", [_R]),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
    end.

%% @doc: After committing/local-committing/aborting a version, try to deal with all versions following that version.
%% Essentially, if this transaction is being committed/local-committed, the function iterates over versions following
%% the current version and aborts them if possible. Then the next following version may be unblocked.
%% If this transaction is being aborted, this function reduces the block count of ist next version and checks whether
%% it is possible to unblock that version. 
deal_pending_records([], {LastReaderTime, FirstPrepTime, PrepNum}, _, DepDict, _, Readers, _, NumRemovePrep, _) ->
    {[], {LastReaderTime, FirstPrepTime, PrepNum-NumRemovePrep}, ignore, Readers, DepDict};
deal_pending_records([{repl_prepare, _TxId, PPTime, _Value, _PendingReaders}|_]=List, {LastReaderTime, FirstPrepTime, PrepNum}, _SCTime, DepDict, _MyNode, Readers, slave, RemovePrepNum, _) ->
    {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict}; 
deal_pending_records([{Type, TxId, PPTime, _Value, PendingReaders}|PWaiter]=List, Metadata, SCTime, 
            DepDict, MyNode, Readers, PartitionType, RemovePrepNum, RemoveDepType) ->
  %lager:warning("Dealing with ~p, Type is ~p, ~p, commit time is ~p", [TxId, Type, PPTime, SCTime]),
    case SCTime > TxId#tx_id.snapshot_time of
        true ->
            %% Abort the current txn
            NewDepDict = case dict:find(TxId, DepDict) of
                            {ok, {_, _, Sender}} ->
                                gen_server:cast(Sender, {aborted, TxId}),
                                dict:erase(TxId, DepDict);
                            {ok, {_, _, _, Sender, _Type, _WriteSet}} ->
                                %lager:warning("Prepare not valid anymore! For ~p, abort to ~p, Type is ~w", [TxId, Sender, Type]),
                                gen_server:cast(Sender, {aborted, TxId, MyNode}),
                                case PartitionType of master -> master_vnode:abort([MyNode], TxId); _ -> ok end,
                                dict:erase(TxId, DepDict);
                            {ok, {_, _, _, Sender, local}} ->
                                gen_server:cast(Sender, {aborted, TxId}),
                                case PartitionType of master -> master_vnode:abort([MyNode], TxId); _ -> ok end,
                                dict:erase(TxId, DepDict);
                            error ->
                                DepDict
                        end,
            case Type of
                local_commit ->
                    deal_pending_records(PWaiter, Metadata, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum, RemoveDepType);
                prepared ->
                    deal_pending_records(PWaiter, Metadata, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum+1, RemoveDepType)
            end;
        false ->
            case Type of 
                master ->
                    case dict:find(TxId, DepDict) of
                        error ->
                            deal_pending_records(PWaiter, Metadata, SCTime, DepDict, MyNode, [], PartitionType, RemovePrepNum, RemoveDepType);
                        _ ->
                            {Partition, _} = MyNode,
                            DepDict1 = case RemoveDepType of
                                           not_remove -> DepDict;
                                           remove_pd -> 
                                                case RemovePrepNum of 
                                                    0 -> unblock_prepare(TxId, DepDict, Partition, RemoveDepType);
                                                    _ -> unblock_prepare(TxId, DepDict, Partition, remove_ppd)
                                                end;
                                            _ ->
                                                unblock_prepare(TxId, DepDict, Partition, RemoveDepType)
                                       end,
                            {LastReaderTime, FirstPrepTime, PrepNum} = Metadata,
                            {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict1}
                    end;
                _ -> %% slave or cache
                    {Partition, _} = MyNode,
                    DepDict1 = case RemoveDepType of
                                   not_remove -> DepDict;
                                   remove_pd -> 
                                        case RemovePrepNum of 
                                            0 -> unblock_prepare(TxId, DepDict, Partition, RemoveDepType);
                                            _ -> unblock_prepare(TxId, DepDict, Partition, remove_ppd)
                                        end;
                                    _ ->
                                        unblock_prepare(TxId, DepDict, Partition, RemoveDepType)
                               end,
                    {LastReaderTime, FirstPrepTime, PrepNum} = Metadata,
                    {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict1}
            end
    end.


%% @doc: Try to reduce the block counter or unblock blocked a transaction, and checks whether it is 
%% possible to unblock (prepare or pend_prepare that version). 
unblock_prepare(TxId, DepDict, _Partition, convert_to_pd) ->
   %lager:warning("Trying to unblocking transaction ~p", [TxId]),
    case dict:find(TxId, DepDict) of
        {ok, {PendPrepDep, PrepareTime, Sender}} ->
           %lager:warning("PendPrepDep is ~w", [PendPrepDep]),
            case PendPrepDep of
                1 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), dict:erase(TxId, DepDict); 
                _ -> dict:store(TxId, {PendPrepDep-1, PrepareTime, Sender}, DepDict) 
            end;
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
            case RepMode of local -> gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime});
                            _ -> ok
            end,
            dict:store(TxId, {0, PrepDep+1, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
        {ok, {N, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
            %lager:warning("~p updates dep to ~p", [TxId, N-1]),
            dict:store(TxId, {N-1, PrepDep+1, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
        error -> %lager:warning("Unblock txn: no record in dep dict!"),  
            DepDict
    end;
%% Reduce prepared dependency 
unblock_prepare(TxId, DepDict, Partition, RemoveDepType) ->
   %lager:warning("Trying to unblocking prepared transaction ~p, RemveDepType is ~p, dep dict is ~w", [TxId, RemoveDepType, DepDict]),
    case dict:find(TxId, DepDict) of
        {ok, {PendPrepDep, PrepareTime, Sender}} ->
           %lager:warning("~p Removing in slave replica", [TxId]),
            case PendPrepDep of
                1 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), dict:erase(TxId, DepDict); 
                _ -> dict:store(TxId, {PendPrepDep-1, PrepareTime, Sender}, DepDict) 
            end;
        {ok, {0, 1, PrepareTime, Sender, local}} ->
           %lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {solve_pending_prepared, TxId, PrepareTime, {node(), self()}}),
            dict:erase(TxId, DepDict);
        {ok, {0, N, PrepareTime, Sender, local}} ->
           %lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            dict:store(TxId, {0, N-1, PrepareTime, Sender, local}, DepDict);
        {ok, {0, 1, PrepareTime, Sender, RepMode, TxWriteSet}} ->
           %lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {solve_pending_prepared, TxId, PrepareTime, {node(), self()}}),
            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime}, 
            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg), 
            dict:erase(TxId, DepDict);
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, TxWriteSet}} ->
           %lager:warning("PrepDep is ~w", [PrepDep]),
            case RemoveDepType of 
                remove_ppd ->
                   %lager:warning("~p Removing ppd, PrepDep is ~w", [TxId, PrepDep]),
                    case PrepDep of 
                        0 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), 
                            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime},
                            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                            dict:erase(TxId, DepDict);
                        _ ->
                            gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime}),
                            case RepMode of 
                                local ->
                                    RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime},
                                    repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                                    dict:store(TxId, {0, PrepDep, PrepareTime, Sender, RepMode}, DepDict);
                                _ ->
                                    dict:store(TxId, {0, PrepDep, PrepareTime, Sender, RepMode, TxWriteSet}, DepDict)
                            end
                    end;
                remove_pd -> 
                   %lager:warning("~p Removing pd", [TxId]),
                    dict:store(TxId, {1, PrepDep-1, PrepareTime, Sender, RepMode, TxWriteSet}, DepDict)
            end;
        {ok, {PendPrepDep, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
          %lager:warning("~w Herre", [TxId]),
            case RemoveDepType of 
                remove_ppd -> dict:store(TxId, {PendPrepDep-1, PrepDep, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
                remove_pd -> dict:store(TxId, {PendPrepDep, PrepDep-1, PrepareTime, Sender, RepMode, WriteSet}, DepDict)
            end;
        error ->%lager:warning("Unblock txn: no record in dep dict!"),  
            DepDict
    end.

%% @doc: local commit a transaction, by converting all its prepared records to local-committed state and
%% replace the prepare timestamp with a local-commit timestamp. Then all local initialized transactions 
%% can read these records, if their read snapshot is larger than these local-commit timestamp.
local_commit([], _TxId, _SCTime, _InMemoryStore, _PreparedTxs, DepDict, _Partition, _, _, _PartitionType) ->
    DepDict;
local_commit([Key|Rest], TxId, SCTime, InMemoryStore, PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType) ->
    MyNode = {Partition, node()},
   %lager:warning("Trying to insert key ~p with for ~p, specula commit time is ~p", [Key, TxId, SCTime]),
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {LastReaderTime, LastPrepTime, PrepNum}, [{prepared, TxId, PrepareTime, Value, PendingReaders}|Deps]=_Record}] ->
          %lager:warning("In prep, PrepNum is ~w, record is ~w", [PrepNum, _Record]),
            {StillPend, ToPrev} = reply_local_commit(PartitionType, PendingReaders, SCTime, {LOC, FFC, Value}, TxId),
            case ToPrev of
                [] ->
                    ets:insert(PreparedTxs, [{Key, {LastReaderTime, LastPrepTime, PrepNum-1}, [{local_commit, TxId, PrepareTime, 
                            {LOC, FFC, Value}, StillPend}|Deps] }]),
                    local_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType);
                _ ->
                   %lager:warning("In multi read version, Key is ~w, Deps is ~w, ToPrev is ~w", [Key, Deps, ToPrev]),
                    AfterReadRecord = find_version_when_unblock(Key, Deps, ToPrev, InMemoryStore),
                    ets:insert(PreparedTxs, [{Key, {LastReaderTime, LastPrepTime, PrepNum-1}, [{local_commit, TxId, PrepareTime, 
                            {LOC, FFC, Value}, StillPend}|AfterReadRecord] }]),
                    local_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType)
            end;
        [{Key, Metadata, RecordList}] ->
          %lager:warning("SC commit for ~w, ~p, meta data is ~w", [TxId, Key, Metadata]),
            case find_prepare_record(RecordList, TxId) of
                [] -> 
                   %lager:warning("Did not find record! Record list is ~w", [RecordList]),
                    local_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType);
                {Prev, {TxId, TxPrepTime, TxSCValue, PendingReaders}, RestRecords} ->
                    {RemainRecords, Metadata1, _, AbortedReaders, DepDict1} = deal_pending_records(Prev, Metadata, SCTime, DepDict, MyNode, [], PartitionType, 1, convert_to_pd),
                   %lager:warning("Found record! Prev is ~w, Newmeta is ~w, RemainRecords is ~w", [Prev, Metadata1, RemainRecords]),
                    {StillPend, ToPrev} = reply_local_commit(PartitionType, PendingReaders++AbortedReaders, SCTime, {LOC, FFC, TxSCValue}, TxId),
                    AfterReadRecord = find_version_when_unblock(Key, RestRecords, ToPrev, InMemoryStore),
                    true = TxPrepTime =< SCTime,
                    case AfterReadRecord of
                        [] ->
                            {LastReaderTime, TxPrepTime, NewPrepNum} = Metadata1,
                            ets:insert(PreparedTxs, {Key, {LastReaderTime, SCTime, NewPrepNum}, RemainRecords++[{local_commit, TxId, SCTime, {LOC, FFC, TxSCValue}, StillPend}]});
                        _ ->
                            ets:insert(PreparedTxs, {Key, Metadata1, RemainRecords++[{local_commit, TxId, SCTime, {LOC, FFC, TxSCValue}, StillPend}|AfterReadRecord]})
                    end,
                    local_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                        DepDict1, Partition, LOC, FFC, PartitionType)
            end;
        R ->
            lager:error("The txn is actually aborted already ~w, ~w", [Key, R]),
            local_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                DepDict, Partition, LOC, FFC, PartitionType)
    end.

%% @doc: Reply pending readers that are blocked to a prepared version, which is local-committed now.
reply_local_commit(PartitionType, PendingReaders, SCTime, MValue, PreparedTxId) ->
   %lager:warning("Replying specula commit: pendiing readers are ~w", [PendingReaders]),
    case PartitionType of
        cache ->
              ToPrev = lists:foldl(fun({ReaderTxId, _, Sender}=Reader, TP) ->
                        SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                        case SnapshotTime >= SCTime of
                            true ->
                                {PLOC, PFFC, Value} = MValue,
                                %lager:warning("Trying to reply"),
                                case is_safe_read(PLOC, PFFC, PreparedTxId, ReaderTxId) of
                                    true -> gen_server:reply(Sender, {ok, Value}), TP;
                                    false -> 
                                        gen_server:cast(ReaderTxId#tx_id.server_pid, 
                                            {read_blocked, ReaderTxId, PLOC, PFFC, Value, Sender}),
                                        TP
                                end;
                            false ->
                                [Reader|TP]
                        end end,
                    [], PendingReaders),
                {[], ToPrev};
        _ ->
            lists:foldl(fun({ReaderTxId, Sender}=ReaderInfo, {Pend, ToPrev}) ->
                case ReaderTxId#tx_id.snapshot_time >= SCTime of
                    true ->
                        case sc_by_local(ReaderTxId) of
                            true ->
                                {PLOC, PFFC, Value} = MValue,
                                case is_safe_read(PLOC, PFFC, PreparedTxId, ReaderTxId) of
                                    true -> gen_server:reply(Sender, {ok, Value});
                                    false -> 
                                        gen_server:cast(ReaderTxId#tx_id.server_pid, 
                                            {read_blocked, ReaderTxId, PLOC, PFFC, Value, Sender})
                                        %% Maybe place in waiting queue instead of reading it?
                                end,
                                {Pend, ToPrev};
                            false ->
                                 %lager:warning("Adding ~w to pend", [Sender]),
                                {[ReaderInfo|Pend], ToPrev}
                        end;
                    false ->
                             %lager:warning("Adding ~w to to-prev", [Sender]),
                            {Pend, [ReaderInfo|ToPrev]}
                end
            end, {[], []}, PendingReaders)
    end.

%% @doc: reply to all readers that were blocked when trying to read a prepared version,
%% which is committed now. 
reply_to_all(PartitionType, PendingReaders, CommitTime, Value) ->
    case PartitionType of
        cache ->
              lists:foldl(fun({ReaderTxId, _Node, Sender}=ReaderInfo, TP) ->
                        SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                        case SnapshotTime >= CommitTime of
                            true ->
                                gen_server:reply(Sender, {ok, Value}),
                                TP;
                            false -> [ReaderInfo|TP]
                        end end,
                    [], PendingReaders);
        _ ->
            lists:foldl(fun({RTxId, Sender}=ReaderInfo, ToPrev) ->
                case RTxId#tx_id.snapshot_time >= CommitTime of
                    true ->
                        case Sender of
                            {remote, Client} -> 
                                gen_server:cast(RTxId#tx_id.server_pid, {rr_value, RTxId, Client, CommitTime, Value});
                            _ -> gen_server:reply(Sender, {ok, Value})
                        end,
                        ToPrev;
                    false ->
                        [ReaderInfo|ToPrev]
                end
            end, [], PendingReaders)
    end.

%% @doc: Find the appropriate prepared version that should be local committed now.
find_prepare_record(RecordList, TxId) ->
    find_prepare_record(RecordList, TxId, []).
  
find_prepare_record([], _TxId, _Prev) ->
    [];
find_prepare_record([{prepared, TxId, TxPrepTime, Value, Readers}|Rest], TxId, Prev) ->
    {Prev, {TxId, TxPrepTime, Value, Readers}, Rest};
find_prepare_record([Record|Rest], TxId, Prev) ->
    find_prepare_record(Rest, TxId, [Record|Prev]).

%% @doc: Find an appropriate version that can be speculatively read. 
%% Can either speculatively read this version or get blocked (if the version is prepared or
%% the transaction found a local-committed versin but it is not reading locally).
specula_read(TxId, Key, PreparedTxs, SenderInfo) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, {LastReaderTime, FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}| PendingPrepare]}] ->
            case SnapshotTime >= PrepareTime of
                true ->
                    %% Read current version
                    case (Type == local_commit) and sc_by_local(TxId) of
                        true ->
                            {PLOC, PFFC, Value} = MValue,
                            case is_safe_read(PLOC, PFFC, PreparedTxId, TxId) of
                                true -> {specula, Value}; 
                                false -> 
                                        Sender = case SenderInfo of {_, _, S} -> S; {_, S} -> S end,   
                                        gen_server:cast(TxId#tx_id.server_pid, 
                                            {read_blocked, TxId, PLOC, PFFC, Value, Sender}),
                                        wait
                            end;
                        false ->
                           %lager:warning("~p can not read this version, not by local or not specula commit, Type is ~p, PrepTx is ~p, PendPrepNum is ~w", [TxId, Type, PreparedTxId, PendPrepNum]),
                            ets:insert(PreparedTxs, {Key, {max(SnapshotTime, LastReaderTime), FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, [SenderInfo|PendingReader]}| PendingPrepare]}),
                            not_ready
                    end;
                false ->
                    case SnapshotTime < FirstPrepTime of
                        true ->
                            ready;
                        false ->
                            %% Read previous version
                           %lager:warning("Trying to read appr version, pending preapre is ~w", [PendingPrepare]),
                            {IfReady, Record} = find_version_when_read(TxId, PendingPrepare, [], SenderInfo),
                            case IfReady of
                                not_ready ->
                                    ets:insert(PreparedTxs, [{Key, {max(SnapshotTime, LastReaderTime), FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}|Record]}]),
                                    not_ready;
                                wait ->
                                    wait;
                                specula ->
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, {Key, {SnapshotTime, FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}| PendingPrepare]});
                                        false -> ok
                                    end,
                                    SCValue = Record,
                                    {specula, SCValue};
                                ready ->
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, {Key, {SnapshotTime, FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}| PendingPrepare]});
                                        false -> ok
                                    end,
                                    ready
                            end
                    end
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

%% @doc: Unblock blocked readers, when committing/aborting a transaction, i.e. deleting
%% its records from prepared version chain.
delete_and_read(_DeleteType, _, _, _, _Key, DepDict, _, _, [], _TxId, _, _, _) ->
    %lager:warning("Want to ~w ~w for key ~w, but arrived already", [_DeleteType, _TxId, _Key]),
    DepDict;
delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, [{Type, TxId, _Time, MValue, PendingReaders}|Rest], TxId, Prev, Metadata, 0) ->
  %lager:warning("Delete and read ~w for ~w, prev is ~w, metadata is ~w", [DeleteType, TxId, Prev, Metadata]),
    ToRemovePrep = case Type of prepared -> 1; repl_prepare -> 1; _ -> 0 end,
    RemoveDepType = case DeleteType of 
                        commit ->
                            case Type of local_commit -> remove_pd; _ -> remove_ppd end;
                        abort ->
                            case Rest of 
                                [] -> case Type of local_commit -> remove_pd; _ -> remove_ppd end;
                                [{prepared, _, _, _, _}|_] -> not_remove;
                                [{repl_prepare, _, _, _, _}|_] -> not_remove; 
                                [{local_commit, _, _, _, _}|_] ->
                                    case Type of local_commit -> not_remove; _ -> convert_to_pd end
                            end
                    end,
    {RemainPrev, Metadata1, LastPrepTime, AbortReaders, DepDict1} 
        = deal_pending_records(Prev, Metadata, TxCommitTime, DepDict, {Partition, node()}, [], PartitionType, ToRemovePrep, RemoveDepType),
    Value = case Type of local_commit -> {_, _, V} = MValue, V; _ -> MValue end, 
    ToPrev = case DeleteType of 
                abort -> AbortReaders++PendingReaders; 
                commit -> reply_to_all(PartitionType, AbortReaders++PendingReaders, TxCommitTime, Value)
             end,
    AfterReadRecord = find_version_when_unblock(Key, Rest, ToPrev, InMemoryStore),
    case AfterReadRecord of
        [] ->
            Rest = [],
            case RemainPrev of
                [] ->
                    {LastReaderTime, _, 0} = Metadata1,
                    ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    DepDict1;
                _ ->
                    {LastReaderTime, _OldFirstPrep, NewPrepNum} = Metadata1,
                    ets:insert(PreparedTxs, {Key, {LastReaderTime, LastPrepTime, NewPrepNum}, RemainPrev}),
                    DepDict1
            end;
        _ ->
            ets:insert(PreparedTxs, {Key, Metadata1, RemainPrev++AfterReadRecord}),
            DepDict1
    end;
delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, [Current|Rest], TxId, Prev, FirstOne, CAbortPrep) ->
    delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, Rest, TxId, [Current|Prev], FirstOne, CAbortPrep);
delete_and_read(abort, _PreparedTxs, _InMemoryStore, _TxCommitTime, _Key, DepDict, _PartitionType, _MyNode, [], _TxId, _Prev, _Whatever, 0) ->
    %lager:warning("Abort but got nothing"),
    DepDict.

%% @doc: Find an appropriate version to read. This happens when the latest version of
%% the prepared version chain can not be read by the current transaction. 
find_version_when_read(_ReaderTxId, [], _Prev, _SenderInfo) ->
    {ready, []}; 
find_version_when_read(ReaderTxId, [{Type, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], Prev, SenderInfo) when ReaderTxId#tx_id.snapshot_time >= SCTime ->
    case Type of
        prepared ->
            {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]};
        repl_prepare ->
            {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]};
        local_commit ->
            case sc_by_local(ReaderTxId) of
                true ->
                   %lager:warning("~p reads specula value ~p!", [SCTxId, SCValue]),
                    {PLOC, PFFC, Value} = SCValue,
                    case is_safe_read(PLOC, PFFC, SCTxId, ReaderTxId) of
                        true -> {specula, Value}; 
                        false -> 
                                Sender = case SenderInfo of {_, _, S} -> S; {_, S} -> S end,   
                                gen_server:cast(ReaderTxId#tx_id.server_pid, 
                                    {read_blocked, ReaderTxId, PLOC, PFFC, Value, Sender}),
                                {wait, []} 
                    end;
                false ->
                   %lager:warning("~p can read specula value, because not by local!", [SCTxId]),
                    {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]}
            end
    end;
find_version_when_read(ReaderTxId, [H|Rest], Prev, SenderInfo) -> 
    find_version_when_read(ReaderTxId, Rest, [H|Prev], SenderInfo).

%% @doc: Find the appropriate version to read for blocked readers, when a transaction 
%% commits/local-commits/aborts. 
find_version_when_unblock(_Key, List, [], _) -> 
    List;
find_version_when_unblock({_, RealKey}, [], SenderInfos, ignore) ->
    lists:foreach(fun({ReaderTxId, Node, Sender}) ->
            %lager:warning("Send read of ~w from ~w to ~w", [RealKey, ReaderTxId, Sender]),
            master_vnode:remote_read(Node, RealKey, ReaderTxId, Sender)
                  end, SenderInfos),
    [];
find_version_when_unblock(Key, [], SenderInfos, InMemoryStore) -> 
    Value = case ets:lookup(InMemoryStore, Key) of
                [] ->
                    [];
                [{Key, ValueList}] ->
                    [{_CommitTime, First}|_T] = ValueList,
                    First
            end,
    lists:foreach(fun({_ReaderTxId, Sender}) ->
                        gen_server:reply(Sender, {ok, Value})
                  end, SenderInfos),
    [];
find_version_when_unblock(Key, [{local_commit, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], SenderInfos, InMemoryStore) -> 
    RemainReaders = lists:foldl(fun(Reader, Pend) ->
            {TxId, Sender} = case Reader of {RTxId, _, S} -> {RTxId, S}; {RTxId, S} -> {RTxId, S} end,
            case TxId#tx_id.snapshot_time >= SCTime of
                true ->
                    {PLOC, PFFC, Value} = SCValue,
                    case is_safe_read(PLOC, PFFC, SCTxId, TxId) of
                        true -> gen_server:reply(Sender, {ok, Value});
                        false ->
                                gen_server:cast(TxId#tx_id.server_pid, 
                                    {read_blocked, TxId, PLOC, PFFC, Value, Sender})
                    end,
                    Pend;
                false ->
                    [Reader|Pend] 
            end end, [], SenderInfos),
    [{local_commit, SCTxId, SCTime, SCValue, SCPendingReaders}|find_version_when_unblock(Key, Rest, RemainReaders, InMemoryStore)];
find_version_when_unblock(_Key, [{Type, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], SenderInfos, _InMemoryStore) -> 
    [{Type, SCTxId, SCTime, SCValue, SenderInfos++SCPendingReaders}|Rest].

sc_by_local(TxId) ->
    node(TxId#tx_id.server_pid) == node().

%% @doc: Insert prepare records into version, when a slave replica receives replicated prepare
%% records from a master replica. 
insert_prepare(PreparedTxs, TxId, Partition, WriteSet, TimeStamp, Sender) ->
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
          [] ->
              {KeySet, ToPrepTS} = lists:foldl(fun({Key, _Value}, {KS, Ts}) ->
                                          case ets:lookup(PreparedTxs, Key) of
                                              [] -> {[Key|KS], Ts};
                                              [{Key, {LastReaderTS, _, _}, [{_Type, _PrepTxId, _, _, _}|_Rest]}] ->
                                                  {[Key|KS], max(Ts, LastReaderTS+1)};
                                              [{Key, LastReaderTS}] ->  
                                                  {[Key|KS], max(Ts, LastReaderTS+1)}
                                          end end, {[], TimeStamp}, WriteSet),
              lists:foreach(fun({Key, Value}) ->
                          case ets:lookup(PreparedTxs, Key) of
                              [] ->
                                  true = ets:insert(PreparedTxs, {Key, {ToPrepTS, ToPrepTS, 1}, [{repl_prepare, TxId, ToPrepTS, Value, []}]});
                              [{Key, LastReaderTime}] -> 
                                  true = ets:insert(PreparedTxs, {Key, {max(LastReaderTime, ToPrepTS), ToPrepTS, 1}, [{repl_prepare, TxId, ToPrepTS, Value, []}]});
                              [{Key, {LastReaderTS, LastSCTime, PrepNum}, List}] ->
                                  NewList = add_to_list(TxId, ToPrepTS, Value, List),
                                  true = ets:insert(PreparedTxs, {Key, {LastReaderTS, min(LastSCTime, ToPrepTS), PrepNum+1}, NewList})
              end end,  WriteSet),
             %lager:warning("Got repl prepare for ~p, propose ~p and replied", [TxId, ToPrepTS]),
              ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
              gen_server:cast(Sender, {solve_pending_prepared, TxId, ToPrepTS, {node(), self()}});
          _R ->
              %lager:warning("Not replying for ~p, ~p because already prepard, Record is ~p", [TxId, Partition, _R]),
              ok
      end.

%% @doc: If the received replicated prepare request can not be added to the head of the list, insert it into
%% the appropriate localtion of the version chain.
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [{Type, PrepTxId, PrepTS, PrepValue, Reader}|Rest] = L) ->
    case ToPrepTS > PrepTS of
        true -> 
            [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}|L];
        false -> 
            [{Type, PrepTxId, PrepTS, PrepValue, Reader} | add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, Rest)]
    end;
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, []) ->
    [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}].

%% @doc: For non-speculative reads, checks whether the transaction can read committed version or should be blocked. 
ready_or_block(TxId, Key, PreparedTxs, SenderInfo) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, {LastReaderTime, FirstPrepTime, PrepNum}, [{Type, PreparedTxId, PrepareTime, Value, PendingReader}|PendingPrepare]}] ->
            case SnapshotTime < FirstPrepTime of
                true ->
                    %lager:warning("Actually ready!"),
                    ready;
                false ->
                  %lager:warning("~p Not ready.. ~p waits for ~p with ~p, FirstPrepTime is ~w, others are ~p, pending prepare ~w", [Key, TxId, PreparedTxId, PrepareTime, FirstPrepTime, PendingReader, PendingPrepare]),
                    case PendingPrepare of
                        [{_Type, _PrepTxId, MyPrepareTime, _PrepValue, _RWaiter}] ->
                            FirstPrepTime = MyPrepareTime;
                        _ -> ok
                    end,
                    case SnapshotTime >= PrepareTime of
                        true ->
                            ets:insert(PreparedTxs, {Key, {LastReaderTime, FirstPrepTime, PrepNum}, [{Type, PreparedTxId, PrepareTime, Value, [SenderInfo|PendingReader]}| PendingPrepare]}),
                            not_ready;
                        false ->
                            Record = insert_pend_reader(PendingPrepare, SnapshotTime, SenderInfo),
                           %lager:warning("After insert, record is ~w", [Record]),
                            ets:insert(PreparedTxs, {Key, {LastReaderTime, FirstPrepTime, PrepNum}, [{Type, PreparedTxId, PrepareTime, Value, PendingReader}|Record]}),
                            not_ready
                    end
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

%% @doc: Insert a blocked reader into the version chain. 
insert_pend_reader([{Type, PrepTxId, PrepareTime, PrepValue, RWaiter}|Rest], SnapshotTime, SenderInfo) when SnapshotTime >= PrepareTime ->
    [{Type, PrepTxId, PrepareTime, PrepValue, [SenderInfo|RWaiter]}|Rest]; 
insert_pend_reader([Record|Rest], SnapshotTime, SenderInfo) ->
    [Record|insert_pend_reader(Rest, SnapshotTime, SenderInfo)].

%% @doc: Check the LOC and FFC of a transaction to see if it can read the current version. 
is_safe_read(PLOC, PFFC, PreparedTxId, TxId) ->
    ets:insert(dependency, {PreparedTxId, TxId}),
    case ets:lookup(anti_dep, TxId) of
        [] ->
            %lager:warning("Insert ~w ~w for ~w, of ~w", [PLOC, PFFC, TxId, PreparedTxId]),
            ets:insert(anti_dep, {TxId, {PLOC, [PLOC]}, PFFC, [PreparedTxId]}),
            true;
        [{TxId, {OldLOC, LOCList}, FFC, Deps}] ->
            NLOC = min(OldLOC, PLOC),
            NFFC = max(FFC, PFFC),
            case NLOC >= NFFC of
                true ->
                    %lager:warning("NewLOC is ~w, NewFFC is ~w, safe!", [NLOC, NFFC]),
                    NLOCList = case PLOC of inf -> LOCList; _ -> [PLOC|LOCList] end,
                    ets:insert(anti_dep, {TxId, {NLOC, NLOCList}, NFFC, [PreparedTxId|Deps]}),
                    true;
                false ->
                    %lager:warning("NewLOC is ~w, NewFFC is ~w, unsafe!", [NLOC, NFFC]),
                    false
            end
    end.

read_value(Key, TxId, Sender, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            gen_server:reply(Sender, {ok, []});
        [{Key, ValueList}] ->
            %lager:warning("~w trying to read ~w", [TxId, Key]),
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock, TxId, Sender)
    end.

%% @doc: Handling read request from a remote node. 
remote_read_value(Key, TxId, Sender, InMemoryStore) ->
     %lager:warning("~w remote reading ~w", [TxId, Key]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            %lager:warning("Directly reply to ~w", [Sender]),
            gen_server:reply(Sender, {ok, []});
        [{Key, ValueList}] ->
            %lager:warning("~w trying to read ~w", [TxId, Key]),
            MyClock = TxId#tx_id.snapshot_time,
            find_version_for_remote(ValueList, MyClock, TxId, Sender)
    end.

find_version([],  _SnapshotTime, _, Sender) ->
    gen_server:reply(Sender, {ok, []});
find_version([{TS, Value}|Rest], SnapshotTime, TxId, Sender) ->
    case SnapshotTime >= TS of
        true ->
            case ets:lookup(anti_dep, TxId) of
                [] ->
                    ets:insert(anti_dep, {TxId, {inf, []}, TS, []}),
                    gen_server:reply(Sender, {ok, Value});
                [{TxId, {LOC, LOCList}, FFC, Deps}] ->
                     %lager:warning("~w has ~w, ~w, ~w, ~w", [TxId, LOC, LOCList, FFC, Deps]),
                    case TS =< FFC of
                        true ->
                            gen_server:reply(Sender, {ok, Value});
                        false ->
                            case TS =< LOC of
                                true ->
                                    ets:insert(anti_dep, {TxId, {LOC, LOCList}, TS, Deps}),
                                    gen_server:reply(Sender, {ok, Value});
                                false ->
                                   %lager:warning("~w blocked when trying to read, TS is ~w, LOC is ~w", [TxId, TS, LOC]),
                                    gen_server:cast(TxId#tx_id.server_pid, {read_blocked, TxId, inf, TS, Value, Sender})
                            end
                    end 
            end;
        false ->
            find_version(Rest, SnapshotTime, TxId, Sender)
    end.


find_version_for_remote([],  _SnapshotTime, _, Sender) ->
    gen_server:reply(Sender, {ok, []});
find_version_for_remote([{TS, Value}|Rest], SnapshotTime, TxId, Sender) ->
    case SnapshotTime >= TS of
        true ->
             %lager:warning("Send ~w of ~w to its coord", [Value, TxId]),
            gen_server:cast(TxId#tx_id.server_pid, {rr_value, TxId, Sender, TS, Value});
        false ->
            find_version_for_remote(Rest, SnapshotTime, TxId, Sender)
    end.
