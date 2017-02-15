% -------------------------------------------------------------------
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
%% @doc: The file for loading configuration parameters.

-module(antidote_config).

-export([load/1,
         set/2,
         get/1, 
         get/2]).

%% ===================================================================
%% Public API
%% ===================================================================

load(File) ->
    FileName = case file:read_file_info(File) of
                    {ok, _} -> File;
                    {error, enoent} -> "../"++File;
                    {error, _R} -> File
               end,
    TermsList =
        case file:consult(FileName) of
              {ok, Terms} ->
                  Terms;
              {error, Reason} ->
                  lager:info("Failed to parse config file ~s: ~p\n", [FileName, Reason])
          end,
    load_config(TermsList),
    notice_info().

set(Key, Value) ->
    ok = application:set_env(antidote, Key, Value).

get(Key) ->
    case application:get_env(antidote, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            io:format("Missing configuration key ~p", [Key])
    end.

get(Key, Default) ->
    case application:get_env(antidote, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

load_config([]) ->
    ok;
load_config([{Key, Value} | Rest]) ->
    ?MODULE:set(Key, Value),
    load_config(Rest);
load_config([ Other | Rest]) ->
    io:format("Ignoring non-tuple config value: ~p\n", [Other]),
    load_config(Rest).

notice_info() ->
    case antidote_config:get(do_repl) of
        true ->
            lager:info("Will do replication");
        false ->
            lager:info("No replication")
    end,
    case antidote_config:get(do_specula) of
        true ->
            lager:info("Will do speculation");
        false ->
            lager:info("No speculation")
    end,
    case antidote_config:get(do_cert) of
        true ->
            lager:info("Will do certification");
        false ->
            lager:info("No certification")
    end.
