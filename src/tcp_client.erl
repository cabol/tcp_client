%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Carlos Andres Bolaños, Inc. All Rights Reserved.
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

%%%-------------------------------------------------------------------
%%% @author Carlos Andres Bolaños R.A. <cabol.dev@gmail.com>
%%% @copyright (C) 2015, <Carlos Andres Bolaños>, All Rights Reserved.
%%% @doc Main interface.
%%%-------------------------------------------------------------------
-module(tcp_client).

%% API
-export([start_link/4, start_link/5]).
-export([sync_send/2, sync_send/3]).
-export([send/2, send/3]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv start_link(PoolName, Size, Address, Port, [])
start_link(PoolName, Size, Address, Port) ->
  start_link(PoolName, Size, Address, Port, []).

%% @doc Creates a pool of TCP connections against the server that
%%      listen on the given `Address':`Port'.
-spec start_link(
  atom(),
  pos_integer(),
  tcp_client_socket:address(),
  tcp_client_socket:portnum(),
  tcp_client_socket:client_options()
) -> gen:start_ret().
start_link(PoolName, Size, Address, Port, Options) ->
  SizeArgs = [{size, Size},
              {max_overflow, 0}],
  PoolArgs = [{name, {local, PoolName}},
              {worker_module, tcp_client_socket}] ++ SizeArgs,
  WorkerArgs = [{address, Address},
                {port, Port},
                {options, Options}],
  poolboy:start_link(PoolArgs, WorkerArgs).

%% @equiv sync_send(PoolName, Msg, Timeout)
sync_send(PoolName, Msg) ->
  execute(PoolName, {sync_send, [Msg]}).

%% @doc Sends a message on the current connection and waits until server
%%      respose arrives. Works as Request/Response.
-spec sync_send(
  atom(), tcp_client_socket:message(), timeout()
) -> tcp_client_socket:sync_reply().
sync_send(PoolName, Msg, Timeout) ->
  execute(PoolName, {sync_send, [Msg, Timeout]}).

%% @equiv send(PoolName, Msg, Timeout)
send(PoolName, Msg) ->
  execute(PoolName, {send, [Msg]}).

%% @doc Sends a message on the current connection but returns immediately
%%      with a request id. Works asynchronously, and if the server sends
%%      a response back, it is sent directly to the caller process.
-spec send(
  atom(), tcp_client_socket:message(), timeout()
) -> tcp_client_socket:reply().
send(PoolName, Msg, Timeout) ->
  execute(PoolName, {send, [Msg, Timeout]}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
execute(PoolName, {Cmd, Args}) ->
  poolboy:transaction(
    PoolName,
    fun(Worker) ->
      apply(tcp_client_socket, Cmd, [Worker | Args])
    end).
