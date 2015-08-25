%%%-------------------------------------------------------------------
%%% @doc
%%% This module was based on Basho's Riak Erlang Client, specifically
%%% from module `riakc_pb_socket'.
%%% @end
%%% @see <a href="https://github.com/basho/riak-erlang-client">Riak Erlang Client</a>
%%%-------------------------------------------------------------------
-module(tcp_client_socket).

-behaviour(gen_server).

%% API
-export([start_link/2, start_link/3,
         is_connected/1,
         sync_send/2, sync_send/3,
         send/2, send/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%%===================================================================
%%% Types and Macros
%%%===================================================================

%% Constants
-define(DEFAULT_TIMEOUT, 10000).
-define(FIRST_RECONNECT_INTERVAL, 100).
-define(MAX_RECONNECT_INTERVAL, 30000).

%% Options for starting or modifying the connection:
%% `queue_if_disconnected' when present or true will cause requests to
%% be queued while the connection is down. `auto_reconnect' when
%% present or true will automatically attempt to reconnect to the
%% server if the connection fails or is lost.
-type client_option()  :: queue_if_disconnected |
                          {queue_if_disconnected, boolean()} |
                          {connect_timeout, pos_integer()} |
                          auto_reconnect |
                          {auto_reconnect, boolean()} |
                          keepalive |
                          {keepalive, boolean()}.

%% A list of client options.
-type client_options() :: [client_option()].

%% Message to be sent
-type message() :: iodata().

%% Request ID - for asynchronous request
-type req_id() :: non_neg_integer().

%% Response
-type sync_reply() :: {ok, Msg :: binary()} | {error, term()}.

%% Request
-record(request, {ref     :: reference(),
                  msg     :: message(),
                  from    :: pid(),
                  ctx     :: any(),
                  timeout :: timeout(),
                  tref    :: reference() | undefined }).

%% The TCP port number of the Server
-type portnum() :: non_neg_integer().

%% The TCP/IP host name or address of Server
-type address() :: string() | atom() | inet:ip_address().

%% Inputs for a MapReduce job.
-type connection_failure() :: {Reason::term(), FailureCount::integer()}.

%% State
-record(state, {% address to connect to
                address :: address(),
                % port to connect to
                port :: portnum(),
                % if true, automatically reconnects to server
                auto_reconnect = false :: boolean(),
                % if false, exits on connection failure/request timeout
                % if true, add requests to queue if disconnected
                queue_if_disconnected = false :: boolean(),
                % gen_tcp socket
                sock :: gen_tcp:socket() | ssl:sslsocket(),
                % if true, enabled TCP keepalive for the socket
                keepalive = false :: boolean(),
                % trasport: TCP | SSL/TLS
                transport = gen_tcp :: gen_tcp | ssl,
                % active request
                active :: #request{} | undefined,
                % queue of pending requests
                queue :: queue:queue() | undefined,
                % number of successful connects
                connects = 0 :: non_neg_integer(),
                % breakdown of failed connects
                failed = [] :: [connection_failure()],
                % timeout of TCP connection
                connect_timeout = infinity :: timeout(),
                % username/password
                credentials :: undefined | {string(), string()},
                % Path to CA certificate file
                cacertfile,
                % Path to client certificate file, when using
                certfile,
                % Path to certificate keyfile, when using
                keyfile,
                % Arbitrary SSL options, see the erlang SSL
                ssl_opts = [],
                % documentation.
                reconnect_interval = ?FIRST_RECONNECT_INTERVAL :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @private Like `gen_server:call/3', but with the timeout hardcoded
%% to `infinity'.
call_infinity(Pid, Msg) ->
  gen_server:call(Pid, Msg, infinity).

%% @doc Create a linked process to talk with the server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> gen:start_ret().
start_link(Address, Port) ->
  start_link(Address, Port, []).

%% @doc Create a linked process to talk with the server on Address:Port
%%      with Options. Client id will be assigned by the server.
-spec start_link(address(), portnum(), client_options()) -> gen:start_ret().
start_link(Address, Port, Options) when is_list(Options) ->
  gen_server:start_link(?MODULE, [Address, Port, Options], []).

%% @doc Determines whether the client is connected. Returns true if connected,
%%      or false and a list of connection failures and frequencies if
%%      disconnected.
-spec is_connected(pid()) -> true | {false, [connection_failure()]}.
is_connected(Pid) ->
  call_infinity(Pid, is_connected).

%% @equiv send(Pid, Msg, ?DEFAULT_TIMEOUT)
-spec sync_send(pid(), message()) -> sync_reply().
sync_send(Pid, Msg) ->
  send(Pid, Msg, ?DEFAULT_TIMEOUT).

%% @doc Sends a message on the current connection and waits until server
%%      respose arrives. Works as Request/Response.
-spec sync_send(pid(), message(), timeout()) -> sync_reply().
sync_send(Pid, Msg, Timeout) ->
  call_infinity(Pid, {req, Msg, Timeout}).

%% @equiv send(Pid, Msg, ?DEFAULT_TIMEOUT)
-spec send(pid(), message()) -> {ok, req_id()}.
send(Pid, Msg) ->
  send(Pid, Msg, ?DEFAULT_TIMEOUT).

%% @doc Sends a message on the current connection but returns immediately
%%      with a request id. Works asynchronously, and if the server sends
%%      a response back, it is sent directly to the caller process.
-spec send(pid(), message(), timeout()) -> {ok, req_id()}.
send(Pid, Msg, Timeout) ->
  ReqId = mk_reqid(),
  gen_server:cast(Pid, {req, Msg, Timeout, {ReqId, self()}}),
  {ok, ReqId}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init([Address, Port, Options]) ->
  %% Schedule a reconnect as the first action.  If the server is up then
  %% the handle_info(reconnect) will run before any requests can be sent.
  State = parse_options(Options, #state{address = Address,
                                        port = Port,
                                        queue = queue:new()}),
  case connect(State) of
    {error, Reason} when State#state.auto_reconnect /= true ->
      {stop, {tcp, Reason}};
    {error, _Reason} ->
      erlang:send_after(State#state.reconnect_interval, self(), reconnect),
      {ok, State};
    Ok ->
      Ok
  end.

%% @hidden
handle_call({req, Msg, Timeout}, From, State)
    when State#state.sock =:= undefined ->
  case State#state.queue_if_disconnected of
    true ->
      {noreply, queue_request(new_request(Msg, From, Timeout), State)};
    false ->
      {reply, {error, disconnected}, State}
  end;
handle_call({req, Msg, Timeout, Ctx}, From, State)
    when State#state.sock =:= undefined ->
  case State#state.queue_if_disconnected of
    true ->
      {noreply, queue_request(new_request(Msg, From, Timeout, Ctx), State)};
    false ->
      {reply, {error, disconnected}, State}
  end;
handle_call({req, Msg, Timeout}, From, State)
    when State#state.active =/= undefined ->
  {noreply, queue_request(new_request(Msg, From, Timeout), State)};
handle_call({req, Msg, Timeout, Ctx}, From, State)
    when State#state.active =/= undefined ->
  {noreply, queue_request(new_request(Msg, From, Timeout, Ctx), State)};
handle_call({req, Msg, Timeout}, From, State) ->
  {noreply, send_request(new_request(Msg, From, Timeout), State)};
handle_call({req, Msg, Timeout, Ctx}, From, State) ->
  {noreply, send_request(new_request(Msg, From, Timeout, Ctx), State)};
handle_call(is_connected, _From, State) ->
  case State#state.sock of
    undefined ->
      {reply, {false, State#state.failed}, State};
    _ ->
      {reply, true, State}
  end;
handle_call({set_options, Options}, _From, State) ->
  {reply, ok, parse_options(Options, State)};
handle_call(stop, _From, State) ->
  disconnect(State),
  {stop, normal, ok, State}.

%% @hidden
handle_cast({req, Msg, Timeout, Ctx}, State)
    when State#state.sock =:= undefined ->
  case State#state.queue_if_disconnected of
    true ->
      Request = new_request(Msg, undefined, Timeout, Ctx),
      {noreply, queue_request(Request, State)};
    false ->
      {noreply, State}
  end;
handle_cast({req, Msg, Timeout, Ctx}, State)
    when State#state.active =/= undefined ->
  {noreply, queue_request(new_request(Msg, undefined, Timeout, Ctx), State)};
handle_cast({req, Msg, Timeout, Ctx}, State) ->
  {noreply, send_request(new_request(Msg, undefined, Timeout, Ctx), State)};
handle_cast(_Msg, State) ->
  {noreply, State}.

%% @hidden
handle_info({tcp_error, _Socket, Reason}, State) ->
  error_logger:error_msg(
    "Client TCP error for ~p:~p - ~p\n",
    [State#state.address, State#state.port, Reason]),
  disconnect(State);
handle_info({tcp_closed, _Socket}, State) ->
  disconnect(State);
handle_info({ssl_error, _Socket, Reason}, State) ->
  error_logger:error_msg(
    "Client SSL error for ~p:~p - ~p\n",
    [State#state.address, State#state.port, Reason]),
  disconnect(State);
handle_info({ssl_closed, _Socket}, State) ->
  disconnect(State);
%% Make sure the two Sock's match.  If a request timed out, but there was
%% a response queued up behind it we do not want to process it.  Instead
%% it should drop through and be ignored.
handle_info({Proto, Sock, Data}, State = #state{sock = Sock, active = Active})
    when Proto == tcp; Proto == ssl ->
  cancel_req_timer(Active#request.tref),
  send_caller({ok, Data}, State#state.active),
  NewState = dequeue_request(State#state{active = undefined}),
  case State#state.transport of
    gen_tcp ->
      ok = inet:setopts(Sock, [{active, once}]);
    ssl ->
      ok = ssl:setopts(Sock, [{active, once}])
  end,
  {noreply, NewState};
handle_info({req_timeout, Ref}, State) ->
  case State#state.active of
    undefined ->
      {noreply, remove_queued_request(Ref, State)};
    Active ->
      case Ref == Active#request.ref of
        true ->
          disconnect(State#state{active = undefined});
        false ->
          {noreply, remove_queued_request(Ref, State)}
      end
  end;
handle_info(reconnect, State) ->
  case connect(State) of
    {ok, NewState} ->
      {noreply, dequeue_request(NewState)};
    {error, Reason} ->
      %% Update the failed count and reschedule a reconnection
      Failed = orddict:update_counter(Reason, 1, State#state.failed),
      NewState = State#state{failed = Failed},
      disconnect(NewState)
  end;
handle_info(_, State) ->
  {noreply, State}.

%% @hidden
terminate(_Reason, _State) -> ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% Parse options
parse_options([], State) ->
  %% Once all options are parsed, make sure auto_reconnect is enabled
  %% if queue_if_disconnected is enabled.
  case State#state.queue_if_disconnected of
    true -> State#state{auto_reconnect = true};
    _    -> State
  end;
parse_options([{connect_timeout, T} | Options], State) when is_integer(T) ->
  parse_options(Options, State#state{connect_timeout = T});
parse_options([{queue_if_disconnected, Bool} | Options], State) when
  Bool =:= true; Bool =:= false ->
  parse_options(Options, State#state{queue_if_disconnected = Bool});
parse_options([queue_if_disconnected | Options], State) ->
  parse_options([{queue_if_disconnected, true} | Options], State);
parse_options([{auto_reconnect, Bool} | Options], State) when
  Bool =:= true; Bool =:= false ->
  parse_options(Options, State#state{auto_reconnect = Bool});
parse_options([auto_reconnect | Options], State) ->
  parse_options([{auto_reconnect, true} | Options], State);
parse_options([{keepalive, Bool} | Options], State) when is_boolean(Bool) ->
  parse_options(Options, State#state{keepalive = Bool});
parse_options([keepalive | Options], State) ->
  parse_options([{keepalive, false} | Options], State);
parse_options([{credentials, User, Pass} | Options], State) ->
  parse_options(Options, State#state{credentials = {User, Pass}});
parse_options([{certfile, File} | Options], State) ->
  parse_options(Options, State#state{certfile = File});
parse_options([{cacertfile, File} | Options], State) ->
  parse_options(Options, State#state{cacertfile = File});
parse_options([{keyfile, File} | Options], State) ->
  parse_options(Options, State#state{keyfile = File});
parse_options([{ssl_opts, Opts} | Options], State) ->
  parse_options(Options, State#state{ssl_opts = Opts}).

%% @private
%% Reply to caller - form clause first in case a ReqId/Client was passed
%% in as the context and gen_server:reply hasn't been called yet.
send_caller({ok, Msg},
            #request{ctx = {ReqId, Client},
            from = undefined} = Request) ->
  Client ! {ReqId, Msg},
  Request;
send_caller(Msg, #request{from = From} = Request) when From /= undefined ->
  gen_server:reply(From, Msg),
  Request#request{from = undefined}.

%% @private
%% Make a new request that can be sent or queued
new_request(Msg, From, Timeout) ->
  new_request(Msg, From, Timeout, undefined).
new_request(Msg, From, Timeout, Context) ->
  Ref = make_ref(),
  #request{ref = Ref,
           msg = Msg,
           from = From,
           ctx = Context,
           timeout = Timeout,
           tref = create_req_timer(Timeout, Ref)}.

%% @private
%% Create a request timer if desired, otherwise return undefined.
create_req_timer(infinity, _Ref) ->
  undefined;
create_req_timer(undefined, _Ref) ->
  undefined;
create_req_timer(Msecs, Ref) ->
  erlang:send_after(Msecs, self(), {req_timeout, Ref}).

%% @private
%% Cancel a request timer made by create_timer/2
cancel_req_timer(undefined) ->
  ok;
cancel_req_timer(Tref) ->
  erlang:cancel_timer(Tref),
  ok.

%% @private
%% Connect the socket if disconnected
connect(State) when State#state.sock =:= undefined ->
  #state{address = Address, port = Port, connects = Connects} = State,
  Conn = gen_tcp:connect(
    Address, Port,
    [binary, {active, once}, {packet, 0}, {keepalive, State#state.keepalive}],
    State#state.connect_timeout),
  case Conn of
    {ok, Sock} ->
      State1 = State#state{sock = Sock,
                           connects = Connects + 1,
                           reconnect_interval = ?FIRST_RECONNECT_INTERVAL},
      case State#state.transport of
        ssl -> start_tls(State1);
        _   -> {ok, State1}
      end;
    Error -> Error
  end.

%% @private
start_tls(State=#state{sock=Sock}) ->
  Options = [{verify, verify_peer}, {cacertfile, State#state.cacertfile}] ++
            [{K, V} || {K, V} <- [{certfile, State#state.certfile},
                                  {keyfile, State#state.keyfile}],
            V /= undefined] ++ State#state.ssl_opts,
  case ssl:connect(Sock, Options, 1000) of
    {ok, SSLSock} ->
      ok = ssl:setopts(SSLSock, [{active, once}]),
      {ok, State};
    {error, Reason2} ->
      {error, Reason2}
  end.

%% @private
%% Disconnect socket if connected
disconnect(State) ->
  %% Tell any pending requests we've disconnected
  case State#state.active of
    undefined ->
      ok;
    Request ->
      send_caller({error, disconnected}, Request)
  end,
  %% Make sure the connection is really closed
  case State#state.sock of
    undefined ->
      ok;
    Sock ->
      Transport = State#state.transport,
      Transport:close(Sock)
  end,
  %% Decide whether to reconnect or exit
  NewState = State#state{sock = undefined, active = undefined},
  case State#state.auto_reconnect of
    true ->
      %% Schedule the reconnect message and return state
      erlang:send_after(State#state.reconnect_interval, self(), reconnect),
      {noreply, increase_reconnect_interval(NewState)};
    false ->
      {stop, disconnected, NewState}
  end.

%% @private
%% Double the reconnect interval up to the maximum
increase_reconnect_interval(State) ->
  case State#state.reconnect_interval of
    Interval when Interval < ?MAX_RECONNECT_INTERVAL ->
      NewInterval = min(Interval + Interval, ?MAX_RECONNECT_INTERVAL),
      State#state{reconnect_interval = NewInterval};
    _ ->
      State
  end.

%% Send a request to the server and prepare the state for the response
%% @private
send_request(Request, State) when State#state.active =:= undefined ->
  {Request1, Pkt} = encode_request_message(Request),
  Transport = State#state.transport,
  case Transport:send(State#state.sock, Pkt) of
    ok ->
      State#state{active = Request1};
    {error, Reason} ->
      error_logger:warning_msg(
        "Socket error while sending request: ~p.", [Reason]),
      Transport:close(State#state.sock),
      maybe_enqueue_and_reconnect(Request1, State#state{sock = undefined})
  end.

%% @private
%% Already encoded (for tunneled messages), but must provide Message Id
%% for responding to the second form of send_request.
encode_request_message(#request{msg = Msg}=Req) ->
  {Req, Msg}.

%% @private
%% If the socket was closed, see if we can enqueue the request and
%% trigger a reconnect. Otherwise, return an error to the requestor.
maybe_enqueue_and_reconnect(Request, State) ->
  maybe_reconnect(State),
  enqueue_or_reply_error(Request, State).

%% @private
%% Trigger an immediate reconnect if automatic reconnection is
%% enabled.
maybe_reconnect(#state{auto_reconnect = true}) ->
  self() ! reconnect;
maybe_reconnect(_) ->
  ok.

%% @private
%% If we can queue while disconnected, do so, otherwise tell the
%% caller that the socket was disconnected.
enqueue_or_reply_error(Request, #state{queue_if_disconnected = true} = State) ->
  queue_request(Request, State);
enqueue_or_reply_error(Request, State) ->
  send_caller({error, disconnected}, Request),
  State.

%% @private
%% Queue up a request if one is pending
queue_request(Request, State) ->
  State#state{queue = queue:in(Request, State#state.queue)}.

%% @private
%% Try and dequeue request and send onto the server if one is waiting
dequeue_request(State) ->
  case queue:out(State#state.queue) of
    {empty, _} ->
      State;
    {{value, Request}, Q2} ->
      send_request(Request, State#state{queue = Q2})
  end.

%% @private
%% Remove a queued request by reference - returns same queue if ref not present
remove_queued_request(Ref, State) ->
  L = queue:to_list(State#state.queue),
  case lists:keytake(Ref, #request.ref, L) of
    false ->
      % Ref not queued up
      State;
    {value, Req, L2} ->
      {reply, Reply, NewState} = on_timeout(Req, State),
      send_caller(Reply, Req),
      NewState#state{queue = queue:from_list(L2)}
  end.

%% Called on timeout for an operation
%% @private
on_timeout(_Request, State) ->
  {reply, {error, timeout}, State}.

%% @private
%% Only has to be unique per-pid
mk_reqid() ->
  erlang:phash2(erlang:now()).
