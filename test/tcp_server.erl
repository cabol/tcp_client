-module(tcp_server).

%% API
-export([server/0, callback/1]).

server() ->
  {ok, LSock} = gen_tcp:listen(5555, [binary, {active, false}]),
  {ok, Sock} = gen_tcp:accept(LSock),
  {ok, Bin} = do_recv(Sock, []),
  ok = gen_tcp:close(Sock),
  Bin.

do_recv(Sock, Bs) ->
  case gen_tcp:recv(Sock, 0) of
    {ok, B} ->
      io:format("~p~n", [B]),
      timer:sleep(2000),
      gen_tcp:send(Sock, B),
      do_recv(Sock, [Bs, B]);
    {error, closed} ->
      {ok, list_to_binary(Bs)}
  end.

callback(Pid) ->
  io:format("[~p] Msg 1: ~p~n", [self(), tcp_client_socket:send(Pid, "Hi!")]),
  timer:sleep(2000),
  io:format("[~p] Msg 2: ~p~n", [self(), tcp_client_socket:send(Pid, "Hi!")]),
  io:format("[~p] Msg 3: ~p~n", [self(), tcp_client_socket:send(Pid, "Hi!")]).
