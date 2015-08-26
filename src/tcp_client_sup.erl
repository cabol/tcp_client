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
%%% @doc
%%% Supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(tcp_client_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Name, Size, Address, Port, Options),
  {
   Name,
   {tcp_client, start_link, [Name, Size, Address, Port, Options]},
   permanent,
   5000,
   worker,
   [tcp_client]
  }
).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init([]) ->
  {ok, Pools} = application:get_env(tcp_client, pools),
  Fun = fun({Name, Size, Options}) ->
          {_, Address} = lists:keyfind(address, 1, Options),
          {_, Port} = lists:keyfind(port, 1, Options),
          {_, Opts} = lists:keyfind(options, 1, Options),
          ?CHILD(Name, Size, Address, Port, Opts)
        end,
  Children = lists:map(Fun, Pools),
  {ok, {{one_for_one, 5, 10}, Children}}.
