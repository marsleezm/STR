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
%% @doc This file implements two functions, such that when committing/aborting
%% a transaction, all transactions that have read from them will be notified.
%%
-module(specula_utilities).

-include("include/speculation.hrl").
-include("include/antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(SEND_MSG(PID, MSG), PID ! MSG).
-else.
-define(SEND_MSG(PID, MSG),  gen_fsm:send_event(PID, MSG)).
-endif.

-export([deal_commit_deps/3, deal_abort_deps/1]).

deal_commit_deps(TxId, LOC, CommitTime) ->
    case ets:lookup(dependency, TxId) of
        [] ->
            ok; %% No read dependency was created!
        List ->
            lists:foreach(fun({TId, DependTxId}) ->
                            ets:delete_object(dependency, {TId, DependTxId}),
                            case DependTxId#tx_id.snapshot_time >= CommitTime of
                                true ->
                                    %lager:info("Calling read valid by ts of ~w, from ~w", [DependTxId, TId]),
                                    gen_server:cast(DependTxId#tx_id.server_pid, {read_valid, DependTxId, TId, LOC});
                                false ->
                                    %lager:info("Calling read invalid by ts of ~w, from ~w", [DependTxId, TId]),
                                    gen_server:cast(DependTxId#tx_id.server_pid, {read_invalid, CommitTime, DependTxId})
                          end end, List)
    end.

deal_abort_deps(TxId) ->
    case ets:lookup(dependency, TxId) of
        [] ->
            ok; %% No read dependency was created!
        List ->
            lists:foreach(fun({TId, DependTxId}) ->
                            %lager:info("Calling read invalid of ~w, from ~w", [DependTxId, TId]),
                            ets:delete_object(dependency, {TId, DependTxId}),
                            gen_server:cast(DependTxId#tx_id.server_pid, {read_invalid, -1, DependTxId})
                          end, List)
    end.
